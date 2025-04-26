#!/usr/bin/env python3

# pylint: disable=too-few-public-methods, missing-docstring

"""Import Slurm accounting database from sacct to sqlite3 database
"""

from __future__ import division, print_function

import argparse
import datetime
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time

__version__ = '0.9.4'

LOG = logging.getLogger('slurm2sql')
LOG.setLevel(logging.DEBUG)
if sys.version_info[0] >= 3:
    logging.lastResort.setLevel(logging.INFO)
else:
    ch = logging.lastResort = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    LOG.addHandler(ch)


#
# First, many converter functions/classes which convert strings to
# useful values.
#

# Single converter functions: transform one column to sqlite value
# stored as that same column.
def settype(type):
    """Decorator: Set type of function, for sql column definitions."""
    def _(x):
        x.type = type
        return x
    return _

@settype('int')
def int_(x):
    """int"""
    return (int(x))

@settype('int')
def nullint(x):
    """int or None"""
    return int(x) if x else None

@settype('text')
def nullstr(x):
    """str or None"""
    return str(x) if x else None

@settype('text')
def nullstr_strip(x):
    """str or None"""
    return str(x).strip() if x else None

@settype('int')
def unixtime(x):
    """Timestamp in local time, converted to unixtime"""
    if not x:           return None
    if x == 'Unknown':  return None
    if x == 'None':  return None
    return time.mktime(time.strptime(x, '%Y-%m-%dT%H:%M:%S'))

@settype('int')
def datetime_timestamp(dt):
    """Convert a datetime object to unixtime

    - Only needed because we support python 2"""
    if hasattr(dt, 'timestamp'): # python3
        return dt.timestamp()
    return time.mktime(dt.timetuple())

@settype('real')
def slurmtime(x):
    """Parse slurm time of format [dd-[hh:]]mm:ss"""
    if not x: return None
    # Handle 'UNLIMITED' ,'Partition_Limit' in 'timelimit' field
    if  x in {'Partition_Limit', 'UNLIMITED'}:
        return None
    seconds = 0
    # The anchor is different if there is '-' or not.  With '-' it is [dd]-hh[:mm[:ss]].  Without it is mm[:ss] first, then hh:mm:ss
    if '-' in x:
        days, time_ = x.split('-', 1)
        seconds += int(days) * 24 * 3600
        hms = time_.split(':')
        if len(hms) >= 1:   seconds += 3600 * int(hms[0])    # hour
        if len(hms) >= 2:   seconds += 60   * float(hms[1])  # min
        if len(hms) >= 3:   seconds +=        int(hms[2])    # sec
    else:
        time_ = x
        hms = time_.split(':')
        # If only a single number is given, it is interperted as minutes
        if len(hms) >= 3:   seconds += 3600 * int(hms[-3])        # hour
        if len(hms) >= 2:   seconds +=        float(hms[-1])       # sec
        if len(hms) >= 1:   seconds += 60   * int(hms[-2] if len(hms)>=2 else hms[-1])  # min
    return seconds

@settype('text')
def slurm_timestamp(x):
    """Convert a datetime to the Slurm format of timestamp
    """
    if not isinstance(x, datetime.datetime):
        x = datetime.datetime.fromtimestamp(x - 5)
    return x.strftime('%Y-%m-%dT%H:%M:%S')

@settype('text')
def str_unknown(x):
    if x == 'Unknown': return None
    return x

@settype('real')
def slurmmem(x):
    """Memory, removing 'n' or 'c' at end, in KB"""
    if not x:  return None
    x = x.strip('nc')
    return float_bytes(x)

# Converting kibi/mibi, etc units to numbers
def unit_value_binary(unit):
    """Convert a unit to its value, e.g. 'K'-->1024, 'M'-->1048576"""
    if unit is None: unit = '_'
    return 2**(10*'_kmgtpezy'.index(unit.lower()))

def unit_value_metric(unit):
    """Convert a unit to its value, e.g. 'K'-->1000, 'M'-->1000000"""
    if unit is None: unit = '_'
    return 1000**('_kmgtpezy'.index(unit.lower()))

@settype('real')
def float_bytes(x, convert=float):
    """Convert a float with unit (K,M, etc) to value"""
    if not x:  return None
    unit = x[-1].lower()
    if unit in 'kmgtpezy':
        return convert(x[:-1]) * unit_value_binary(unit)
    return convert(x)

@settype('int')
def int_bytes(x):
    return float_bytes(x, convert=lambda x: int(float(x)))

@settype('real')
def float_metric(x, convert=float):
    """Convert a float with unit (K,M, etc) to value"""
    if not x:  return None
    unit = x[-1].lower()
    if unit in 'kmgtpezy':
        return convert(x[:-1]) * unit_value_metric(unit)
    return convert(x)

@settype('int')
def int_metric(x):
    return float_metric(x, convert=lambda x: int(float(x)))

# Row converter fuctions which need *all* values to convert.  Classes
# with one method, 'calc', which takes the whole row (a dict) and
# converts it to a sqlite value.  The only point of the class is to be
# able to easily distinguish the function from the column functions
# above.
class linefunc(object):
    """Base class for all converter functions"""
    linefunc = True
    type = ''


# Generic extractor-generator function.
def ExtractField(name, columnname, fieldname, type_, wrap=None):
    """Extract a field out of a column, in the TRES/GRSE column formats.

    Example: 'gres/gpuutil'
    """

    _re = re.compile(rf'\b{fieldname}=([^,]*)\b')
    @staticmethod
    def calc(row):
        if columnname not in row:  return None
        # Slurm 20.11 uses gres= within ReqTRES (instead of ReqGRES)
        #print(row[columnname])
        m = _re.search(row[columnname])
        if m:
            val = type_(m.group(1))
            if wrap:
                val = wrap(val)
            return val

    return type(name, (linefunc,), {'calc': calc, 'type': type_.type})



# The main converter functions


# Submit, start, and end times as unixtimes
class slurmDefaultTime(linefunc):
    @staticmethod
    def calc(row):
        """Latest active time.

        All jobs in sacct are already started, so this is either current
        time or end time.
        """
        if row['End'] != 'Unknown':
            return row['End']
        if row['Start'] != 'Unknown':
            # Currently running, return current time since it's constantly updated.
            return time.strftime("%Y-%m-%dT%H:%M:%S")
        # Return submit time, since there is nothing else.
        return row['Submit']

class slurmDefaultTimeTS(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        """Lastest active time (see above), unixtime."""
        return unixtime(slurmDefaultTime.calc(row))

class slurmSubmitTS(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        return unixtime(row['Submit'])

class slurmStartTS(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        return unixtime(row['Start'])

class slurmEndTS(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        return unixtime(row['End'])

class slurmQueueTime(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        submit = unixtime(row['Submit'])
        start = unixtime(row['Start'])
        if submit is not None and start is not None:
            return start - submit

billing_re = re.compile(r'billing=(\d+)')
class slurmBilling(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        tres = row['AllocTRES']
        if not tres:  return None
        m = billing_re.search(tres)
        if m:
            return int(m.group(1))

# Memory stuff
class slurmMemNode(linefunc):
    """Memory per node.  ReqMem is total across all nodes"""
    type = 'real'
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if not reqmem:  return None
        ncpus = int(row['NCPUS'])
        if ncpus == 0:  return 0
        nnodes = int(row['NNodes'])
        if nnodes == 0: return None
        return slurmmem(reqmem) / nnodes

class slurmMemCPU(linefunc):
    """Memory per cpu, computed if necessary"""
    type = 'real'
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if not reqmem:  return None
        nnodes = int(row['NNodes'])
        if nnodes == 0: return None
        ncpus = int(row['NCPUS'])
        if ncpus == 0:  return None
        return slurmmem(reqmem) / ncpus

class slurmMemType(linefunc):
    """Memory type: 'n' per node, 'c' per core"""
    type = 'real'
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if not reqmem: return None
        if reqmem.endswith('n'):  return 'n'
        if reqmem.endswith('c'):  return 'c'
        return None # latest slurm seems to not have this, ~2021-2022

class slurmMemRaw(linefunc):
    """Raw value of ReqMem column, with 'c' or 'n' suffix"""
    @staticmethod
    def calc(row):
        return row['ReqMem']

# GPU stuff
gpu_re = re.compile(r'gpu[:=](\d+)')
class slurmReqGPU(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        if 'ReqGRES' in row:
            gres = row['ReqGRES']
        else:
            gres = row['ReqTRES']
        if not gres:  return None
        # Slurm 20.11 uses gres= within ReqTRES (instead of ReqGRES)
        m = gpu_re.search(gres)
        if m:
            return int(m.group(1))

class slurmGPUMem(linefunc):
    type = 'real'
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip():  return
        if 'No GPU stats' in comment:  return
        if comment == 'abort': return
        try:
            comment = json.loads(comment)
        except:
            return None
        if not isinstance(comment, dict) or 'gpu_mem_max' not in comment:
            return
        return comment.get('gpu_mem_max') * (2**20)

class slurmGPUEff(linefunc):
    type = 'real'
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip(): return
        if 'No GPU stats' in comment:  return
        if comment == 'abort': return
        try:
            comment = json.loads(comment)
        except:
            return None
        if not isinstance(comment, dict) or 'gpu_util' not in comment:
            return
        return comment['gpu_util']/100.

class slurmGPUCountComment(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip():        return
        if 'No GPU stats' in comment:  return
        if comment == 'abort': return
        try:
            comment = json.loads(comment)
        except:
            return None
        if not isinstance(comment, dict) or 'ngpu' not in comment:
            return
        return int(comment.get('ngpu'))


gpu_re2 = re.compile(r'gpu=(\d+)')
class slurmGPUCount(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        tres = row['AllocTRES'] or row['ReqTRES']
        if not tres:  return None
        m = gpu_re2.search(tres)
        if m:
            return int(m.group(1))

RE_TRES_GPU = re.compile(rf'\bgres/gpu=([^,]*)\b')
RE_TRES_GPU_UTIL = re.compile(rf'\bgres/gpuutil=([^,]*)\b')
class slurmGPUEff2(linefunc):
    """Slurm GPU efficiency (using AllocTRES and TRESUsageInTot columns).
    """
    type = 'real'
    @staticmethod
    def calc(row):
        m_used = RE_TRES_GPU_UTIL.search(row['TRESUsageInTot'])
        m_alloc = RE_TRES_GPU.search(row['AllocTRES'])
        if m_alloc and m_used:
            return (float_metric(m_used.group(1)) / 100.) / float_metric(m_alloc.group(1))
        return None

# Job ID related stuff
jobidonly_re = re.compile(r'[0-9]+')
jobidnostep_re = re.compile(r'[0-9]+(_[0-9]+)?')
class slurmJobID(linefunc):
    """The JobID field as slurm gives it, including _ and ."""
    type = 'text'
    @staticmethod
    def calc(row):
        if 'JobID' not in row: return
        return row['JobID']

class slurmJobIDonly(linefunc):
    """The JobID without any . or _.   This is the same for all array tasks/het offsets"""
    type = 'int'
    @staticmethod
    def calc(row):
        if 'JobID' not in row: return
        return int(jobidonly_re.match(row['JobID']).group(0))

class slurmJobIDnostep(linefunc):
    """The JobID without any `.` suffixes.   This is the same for all het offsets"""
    type = 'text'
    @staticmethod
    def calc(row):
        if 'JobID' not in row: return
        return jobidnostep_re.match(row['JobID']).group(0)

class slurmJobIDrawonly(linefunc):
    """The (raw) JobID without any . or _.  This is different for every job in an array."""
    type = 'int'
    @staticmethod
    def calc(row):
        if 'JobIDRaw' not in row: return
        return int(jobidonly_re.match(row['JobIDRaw']).group(0))


arraytaskid_re = re.compile(r'_([0-9]+)')
class slurmArrayTaskID(linefunc):
    """Array task ID, the part after _."""
    type = 'int'
    @staticmethod
    def calc(row):
        if 'JobID' not in row: return
        if '_' not in row['JobID']:  return
        if '[' in row['JobID']:      return
        return int(arraytaskid_re.search(row['JobID']).group(1))

class slurmJobStep(linefunc):
    type = 'text'
    @staticmethod
    def calc(row):
        if 'JobID' not in row: return
        if '.' not in row['JobID']:  return
        return row['JobID'].split('.')[-1]  # not necessarily an integer

# Efficiency stuff
class slurmMemEff(linefunc):
    """Slurm memory efficiency.

    In modern Slurm, this does *not* work because the allocation rows
    have ReqMem, and the task rows have MaxRSS, but slurm2sql handled
    things only one row at a time.  The `eff` view computes this per
    job.
    """
    # https://github.com/SchedMD/slurm/blob/master/contribs/seff/seff
    type = 'real'
    @staticmethod
    def calc(row):
        reqmem_type = slurmMemType.calc(row)
        mem_max = slurmmem(row['MaxRSS'])
        reqmem = slurmmem(row['ReqMem'])
        nnodes = slurmmem(row['NNodes'])
        if not reqmem or mem_max is None:  return
        if reqmem_type == 'c':
            nodemem = reqmem * int(row['NCPUS'])
        elif reqmem_type == 'n':
            nodemem = reqmem
        elif reqmem_type is None:
            nodemem = reqmem / nnodes
        else:
            raise ValueError('unknown memory type: %s'%reqmem_type)
        return mem_max / nodemem

RE_TRES_MEM = re.compile(rf'\bmem=([^,]*)\b')
class slurmMemEff2(linefunc):
    """Slurm memory efficiency (using AllocTRES and TRESUsageInTot columns).

    This *does* work in new enough Slurm.
    """
    # https://github.com/SchedMD/slurm/blob/master/contribs/seff/seff
    type = 'real'
    @staticmethod
    def calc(row):
        m_used = RE_TRES_MEM.search(row['TRESUsageInTot'])
        m_alloc = RE_TRES_MEM.search(row['AllocTRES'])
        if m_alloc and m_used:
            return float_bytes(m_used.group(1)) / float_bytes(m_alloc.group(1))
        return None


class slurmCPUEff(linefunc):
    # This matches the seff tool currently:
    # https://github.com/SchedMD/slurm/blob/master/contribs/seff/seff
    type = 'real'
    @staticmethod
    def calc(row):
        if not ('Elapsed' in row and 'TotalCPU' in row and 'NCPUS' in row):
            return
        walltime = slurmtime(row['Elapsed'])
        if not walltime: return None
        try:
            cpueff = slurmtime(row['TotalCPU']) / (walltime * int(row['NCPUS']))
        except ZeroDivisionError:
            return float('nan')
        return cpueff

class slurmConsumedEnergy(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        if not row['ConsumedEnergyRaw']:  return None
        return int(row['ConsumedEnergyRaw'])

class slurmExitCodeRaw(linefunc):
    type = 'text'
    @staticmethod
    def calc(row):
        if not row['ExitCode']:  return None
        return row['ExitCode']

class slurmExitCode(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        if not row['ExitCode']:  return None
        return int(row['ExitCode'].split(':')[0])

class slurmExitSignal(linefunc):
    type = 'int'
    @staticmethod
    def calc(row):
        if not row['ExitCode']:  return None
        return int(row['ExitCode'].split(':')[1])


# All defined columns and their respective converter functions.  If a
# key begins in a underscore, this is not a Slurm DB field (don't
# query it from sacct), it is computed from other fields in this
# program.  It gets added to our sqlite database without the
# underscore.
COLUMNS = {
    # Basic job metadata
    #  Job IDs are of the forms (from sacct man page):
    #   - JobID.JobStep
    #   - ArrayJobID_ArrayTaskID.JobStep
    # And the below is consistent with this.
    '_JobID': slurmJobID,               # JobID directly as Slurm presents it
                                        # (with '_' and '.')
    '_JobIDnostep': slurmJobIDnostep,   # Integer JobID without '.' suffixes
    '_JobIDonly': slurmJobIDonly,       # Integer JobID without '_' or '.' suffixes
    '_JobStep': slurmJobStep,           # Part after '.'
    '_ArrayTaskID': slurmArrayTaskID,   # Part between '_' and '.'
    '_JobIDRawonly': slurmJobIDrawonly,
                                        # if array jobs, unique ID for each array task,
                                        # otherwise JobID

    #'JobIDRawSlurm': str,              #
    'JobName': nullstr,                 # Free-form text name of the job
    'User': nullstr,                    # Username
    'Group': nullstr,                   # Group
    'Account': nullstr,                 # Account
    'SubmitLine': nullstr,              # SubmitLine (execution command line)
    '_Billing': slurmBilling,           # Billing (from tres)

    # Times and runtime info
    'State': nullstr,                   # Job state
    'Timelimit': slurmtime,             # Timelimit specified by user
    'Elapsed': slurmtime,               # Walltime of the job
    #'_Time': slurmDefaultTime,          # Genalized time, max(Submit, End, (current if started))
    #'Submit': str_unknown,              # Submit time in yyyy-mm-ddThh:mm:ss straight from slurm
    #'Start': str_unknown,               # Same, job start time
    #'End': str_unknown,                 # Same, job end time
    '_Time': slurmDefaultTimeTS,        # unixtime: Genalized time, max(Submit, End, (current if started))
    'Submit': slurmSubmitTS,            # unixtime: Submit
    'Start': slurmStartTS,              # unixtime: Start
    'End': slurmEndTS,                  # unixtime: End
    '_QueueTime': slurmQueueTime,        # seconds, difference between submission and start
    'Partition': nullstr,               # Partition
    '_ExitCodeRaw': slurmExitCodeRaw,   # ExitStatus:Signal
    'ExitCode': slurmExitCode,        # ExitStatus from above, int
    '_ExitSignal': slurmExitSignal,     # Signal from above, int
    'NodeList': nullstr,                # Node list of jobs
    'Priority': nullint,                # Slurm priority (higher = will run sooner)
    '_ConsumedEnergy': slurmConsumedEnergy,

    # Stuff about number of nodes
    'ReqNodes': int_bytes,              # Requested number of nodes
    'NNodes': nullint,                  # Number of nodes (allocated if ran, requested if not yet)
    'AllocNodes': nullint,              # Number of nodes (allocated, zero if not running yet)

    # Miscelaneous requested resources
    'ReqTRES': nullstr,
    'NTasks': nullint,
    #'AllocGRES'
    'AllocTRES': nullstr,
    'TRESUsageInTot': nullstr,
    'TRESUsageOutTot': nullstr,

    # CPU related
    'NCPUS': nullint,                   # === AllocCPUS
    'ReqCPUS': nullint,                 # Requested CPUs
    'AllocCPUS': nullint,               # === NCPUS
    'CPUTime': slurmtime,               # = Elapsed * NCPUS    (= CPUTimeRaw)  (not how much used)
    'TotalCPU': slurmtime,              # = Elapsed * NCPUS * efficiency
    'UserCPU': slurmtime,               #
    'SystemCPU': slurmtime,             #
    '_CPUEff': slurmCPUEff,             # CPU efficiency, should be same as seff
    'MinCPU': slurmtime,                # Minimum CPU used by any task in the job
    'MinCPUNode': nullstr,
    'MinCPUTask': nullstr,

    # Memory related
    '_TotalMem': ExtractField('TotalMem', 'TRESUsageInTot', 'mem', float_bytes),
    '_AllocMem': ExtractField('AllocMem', 'AllocTRES', 'mem', float_bytes),
    '_MemEff': slurmMemEff2,            # Calculated from AllocTRES and TRESUsageInTot
    'ReqMem': float_bytes,              # Requested mem, value from slurm.  Sum across all nodes
    '_ReqMemNode': slurmMemNode,        # Mem per node, computed
    '_ReqMemCPU': slurmMemCPU,          # Mem per cpu, computed
    'AveRSS': slurmmem,
    'MaxRSS': slurmmem,
    'MaxRSSNode': nullstr,
    'MaxRSSTask': nullstr,
    'MaxPages': int_metric,
    'MaxVMSize': slurmmem,

    # Disk related
    'AveDiskRead': int_bytes,
    'AveDiskWrite': int_bytes,
    'MaxDiskRead': int_bytes,
    'MaxDiskWrite': int_bytes,
    '_TotDiskRead': ExtractField('TotDiskRead', 'TRESUsageInTot', 'fs/disk', float_bytes),
    '_TotDiskWrite': ExtractField('TotDiskWrite', 'TRESUsageOutTot', 'fs/disk', float_bytes),


    # GPU related
    #'_ReqGPUS': slurmReqGPU,            # Number of GPUS requested
    '_ReqGPUS': ExtractField('ReqGpus', 'ReqTRES', 'gres/gpu', float_metric),
    'Comment': nullstr_strip,           # Slurm Comment field (at Aalto used for GPU stats)
    #'_GPUMem': slurmGPUMem,             # GPU mem extracted from comment field
    '_GpuEff': slurmGPUEff2,             # GPU utilization (0.0 to 1.0) from AllocTRES()
    #'_NGPU': slurmGPUCount,             # Number of GPUs, extracted from comment field
    '_NGpus': ExtractField('NGpus', 'AllocTRES', 'gres/gpu', float_metric),
    '_GpuUtil': ExtractField('GpuUtil', 'TRESUsageInAve', 'gres/gpuutil', float_metric, wrap=lambda x: x/100.), # can be >100 for multi-GPU.
    '_GpuMem': ExtractField('GpuMem2', 'TRESUsageInAve', 'gres/gpumem', float_metric),
    '_GpuUtilTot': ExtractField('GpuUtilTot', 'TRESUsageInTot', 'gres/gpuutil', float_metric),
    '_GpuMemTot': ExtractField('GpuMemTot',   'TRESUsageInTot', 'gres/gpumem', float_metric),
    }
# Everything above that does not begin with '_' is queried from sacct.
# These extra columns are added (don't duplicate with the above!)
COLUMNS_EXTRA = ['JobID',
                 'JobIDRaw',
                 'ConsumedEnergyRaw',
                 'TRESUsageInAve',
                 'TRESUsageOutTot',
                ]



def main(argv=sys.argv[1:], db=None, raw_sacct=None, csv_input=None):
    """Parse arguments and use the other API"""
    parser = argparse.ArgumentParser(usage='slurm2sql DBNAME [other args] [SACCT_FILTER]')
    parser.add_argument('db', help="Database filename to create or update")
    parser.add_argument('--update', '-u', action='store_true',
                        help="If given, don't delete existing database and "
                             "instead insert or update rows")
    parser.add_argument('--history',
                        help="Scrape dd-hh:mm:ss or [hh:]mm:ss from the past to now (Slurm time format)")
    parser.add_argument('--history-resume', action='store_true',
                        help="Day-by-day collect history, starting from last collection.")
    parser.add_argument('--history-days', type=int,
                        help="Day-by-day collect history, starting this many days ago.")
    parser.add_argument('--history-start',
                        help="Day-by-day collect history, starting on this day.")
    parser.add_argument('--history-end',
                        help="Day-by-day collect history ends on this day.  Must include one "
                             "of the other history options to have any effect.")
    parser.add_argument('--jobs-only', action='store_true',
                        help="Don't include job steps but only the man jobs")
    parser.add_argument('--csv-input',
                        help="Don't parse sacct but import this CSV file.  It's read with "
                             "Python's default csv reader (excel format).  Beware badly "
                             "formatted inputs, for example line breaks in job names.")
    parser.add_argument('--completed', '-c', action='store_true',
                        help=f"Select for completed job states ({COMPLETED_STATES})  You need to specify --starttime (-S) at some point in the past, due to how saccont default works (for example '-S now-1week').  This option automatically sets '-E now'")
    parser.add_argument('--quiet', '-q', action='store_true',
                        help="Don't output anything unless errors")
    parser.add_argument('--verbose', '-v', action='store_true',
                        help="Output more logging info")
    args, sacct_filter = parser.parse_known_args(argv)

    if args.verbose:
        logging.lastResort.setLevel(logging.DEBUG)
    if args.quiet:
        logging.lastResort.setLevel(logging.WARN)
    LOG.debug(args)

    sacct_filter = args_to_sacct_filter(args, sacct_filter)

    # db is only given as an argument in tests (normally)
    if db is None:
        # Delete existing database unless --update/-u is given
        if not (args.update or args.history_resume) and os.path.exists(args.db):
            os.unlink(args.db)
        db = sqlite3.connect(args.db)

    # If --history-days, get just this many days history
    if (args.history is not None
        or args.history_resume
        or args.history_days is not None
        or args.history_start is not None):
        errors = get_history(db, sacct_filter=sacct_filter,
                            history=args.history,
                            history_resume=args.history_resume,
                            history_days=args.history_days,
                            history_start=args.history_start,
                            history_end=args.history_end,
                            jobs_only=args.jobs_only,
                            raw_sacct=raw_sacct,
                            # --history real usage doesn't make sense with csv
                            # (below is just for running tests)
                            csv_input=csv_input)

        create_indexes(db)
    # Normal operation
    else:
        errors = slurm2sql(db, sacct_filter=sacct_filter,
                           update=args.update,
                           jobs_only=args.jobs_only,
                           raw_sacct=raw_sacct,
                           verbose=args.verbose,
                           csv_input=args.csv_input or csv_input)
        create_indexes(db)

    if errors:
        LOG.warning("Completed with %s errors", errors)
        return(1)
    return(0)


def get_history(db, sacct_filter=['-a'],
                history=None, history_resume=None, history_days=None,
                history_start=None, history_end=None,
                jobs_only=False, raw_sacct=None, csv_input=None):
    """Get history for a certain period of days.

    Queries each day and updates the database, so as to avoid
    overloading sacct and causing a failure.

    Returns: the number of errors.
    """
    errors = 0
    now = datetime.datetime.now().replace(microsecond=0)
    today = datetime.date.today()
    if history_resume:
        try:
            start = get_last_timestamp(db)
        except sqlite3.OperationalError:
            import traceback
            traceback.print_exc()
            print()
            print("Error fetching last start time (see above)", file=sys.stderr)
            exit(5)
        start = datetime.datetime.fromtimestamp(start - 5)
    elif history is not None:
        start = now - datetime.timedelta(seconds=slurmtime(history))
    elif history_days is not None:
        start = datetime.datetime.combine(today - datetime.timedelta(days=history_days), datetime.time())
    elif history_start is not None:
        start = datetime.datetime.strptime(history_start, '%Y-%m-%d')
    if history_end is not None:
        stop = datetime.datetime.strptime(history_end, '%Y-%m-%d')
    else:
        stop = now + datetime.timedelta(seconds=6*3600)

    days_ago = (now - start).days
    day_interval = 1
    while start <= stop:
        end = start+datetime.timedelta(days=day_interval)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        new_filter = sacct_filter + [
            '-S', slurm_timestamp(start),
            '-E', slurm_timestamp(end),
            ]
        LOG.debug(new_filter)
        LOG.info("%s %s", days_ago, start.date() if history_days is not None else start)
        errors += slurm2sql(db, sacct_filter=new_filter, update=True, jobs_only=jobs_only,
                            raw_sacct=raw_sacct, csv_input=csv_input)
        db.commit()
        update_last_timestamp(db, update_time=end)
        start = end
        days_ago -= day_interval
    return errors


def sacct(slurm_cols, sacct_filter):
    cmd = ['sacct', '-o', ','.join(slurm_cols), '-P',# '--units=K',
           '--delimiter=;|;',
           #'--allocations',  # no job steps, only total jobs, but doesn't show used resources.
           ] + list(sacct_filter)
    #LOG.debug(' '.join(cmd))
    error_handling = {'errors':'replace'} if sys.version_info[0]>=3 else {}
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE, universal_newlines=True,
                         **error_handling)
    return p.stdout


def create_indexes(db):
    db.execute('CREATE INDEX IF NOT EXISTS idx_slurm_jobidnostep ON slurm (JobIDnostep)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_slurm_start ON slurm (Start)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_slurm_user_start ON slurm (User, Start)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_slurm_time ON slurm (Time)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_slurm_user_time ON slurm (User, Time)')
    db.execute('ANALYZE;')
    db.commit()


def sacct_iter(slurm_cols, sacct_filter, errors=[0], raw_sacct=None):
    """Iterate through sacct, returning rows as dicts"""
    # Read data from sacct, or interpert sacct_filter directly as
    # testdata if it has the attribute 'testdata'
    if raw_sacct:
        # Support tests - raw lines can be put in
        lines = raw_sacct
    else:
        # This is a real filter, read data
        lines = sacct(slurm_cols, sacct_filter)

    # We don't use the csv module because the csv can be malformed.
    # In particular, job name can include newlines(!).  TODO: handle job
    # names with newlines.
    line_continuation = None
    for i, rawline in enumerate(lines):
        if i == 0:
            # header
            header = rawline.strip().split(';|;')
            continue
        # Handle fields that have embedded newline (JobName).  If we
        # have too few fields, save the line and continue.
        if line_continuation:
            rawline = line_continuation + rawline
            line_continuation = None
        line = rawline.strip().split(';|;')
        if len(line) < len(slurm_cols):
            line_continuation = rawline
            continue
        # (end)
        if len(line) > len(slurm_cols):
            LOG.error("Line with wrong number of columns: (want columns=%s, line has=%s)", len(slurm_cols), len(line))
            LOG.error("columns = %s", header)
            LOG.error("rawline = %s", rawline)
            errors[0] += 1
            continue
        line = dict(zip(header, line))
        yield line


def slurm2sql(db, sacct_filter=['-a'], update=False, jobs_only=False,
              raw_sacct=None, verbose=False,
              csv_input=None):
    """Import one call of sacct to a sqlite database.

    db:
    open sqlite3 database file object.

    sacct_filter:
    filter for sacct, list of arguments.  This should only be row
    filters, such as ['-a'], ['-S' '2019-08-01'], and so on.  The
    argument should be a list.  You can't currently filter what columns
    are selected.

    raw_sacct: If given, do not run sacct but use this as the input
    (file-like object)

    Returns: the number of errors
    """
    columns = COLUMNS.copy()


    def infer_type(cd):
        if hasattr(cd, 'type'): return cd.type
        elif cd == str: return 'text'
        return ''
    create_columns = ', '.join('"%s" %s'%(c.strip('_'), infer_type(cd))
                               for c, cd in columns.items())
    create_columns = create_columns.replace('JobID" text', 'JobID" text UNIQUE')
    db.execute('CREATE TABLE IF NOT EXISTS slurm (%s)'%create_columns)
    db.execute('CREATE TABLE IF NOT EXISTS meta_slurm_lastupdate (id INTEGER PRIMARY KEY, update_time REAL)')
    db.execute('CREATE VIEW IF NOT EXISTS allocations AS select * from slurm where JobStep is null;')
    db.execute('CREATE VIEW IF NOT EXISTS steps AS select * from slurm where JobStep is not null;')
    db.execute('CREATE VIEW IF NOT EXISTS eff AS select '
               'JobIDnostep AS JobID, '
               'max(User) AS User, '
               'max(Partition) AS Partition, '
               'Account, '
               'State, '
               'Time, '
               'TimeLimit, '
               'min(Start) AS Start, '
               'max(End) AS End, '
               'max(NNodes) AS NNodes, '
               'ReqTRES, '
               'max(Elapsed) AS Elapsed, '
               'max(NCPUS) AS NCPUS, '
               'max(totalcpu)/max(cputime) AS CPUeff, '  # highest TotalCPU is for the whole allocation
               'max(cputime) AS cpu_s_reserved, '
               'max(totalcpu) AS cpu_s_used, '
               'max(ReqMemNode) AS MemReq, '
               'max(AllocMem) AS AllocMem, '
               'max(TotalMem) AS TotalMem, '
               'max(MaxRSS) AS MaxRSS, '
               'max(MemEff) AS MemEff, '
               'max(AllocMem*Elapsed) AS mem_s_reserved, ' # highest of any job
               'max(NGpus) AS NGpus, '
               'max(NGpus)*max(Elapsed) AS gpu_s_reserved, '
               'max(NGpus)*max(Elapsed)*max(GPUutil) AS gpu_s_used, '
               #'max(GPUutil)/max(NGpus) AS GPUeff, '               # Individual job with highest use (check this)
               'max(GPUEff) AS GPUeff, '               # Individual job with highest use (check this)
               'max(GPUMem) AS GPUMem, '
               'MaxDiskRead, '
               'MaxDiskWrite, '
               'sum(TotDiskRead) as TotDiskRead, '
               'sum(TotDiskWrite) as TotDiskWrite '
               'FROM slurm GROUP BY JobIDnostep')
    #db.execute('PRAGMA journal_mode = WAL;')
    db.commit()
    c = db.cursor()

    slurm_cols = tuple(c for c in list(columns.keys()) + COLUMNS_EXTRA if not c.startswith('_'))

    errors = [ 0 ]
    if csv_input:
        import collections
        import csv
        if not isinstance(csv_input, csv.DictReader):
            csv_input = csv.DictReader(open(csv_input, 'r'))
        def rows():
            for row in csv_input:
                row = collections.defaultdict(str, ((k,v.strip()) for k,v in row.items()))
                yield row
        rows = rows()  # activate the generator
    else:
        rows = sacct_iter(slurm_cols, sacct_filter, raw_sacct=raw_sacct, errors=errors)

    for i, row in enumerate(rows):

        # If --jobs-only, then skip all job steps (sacct updates the
        # mem/cpu usage on the allocation itself already)
        step_id = slurmJobStep.calc(row)
        if jobs_only and step_id is not None:
            continue

        #LOG.debug(row)
        processed_row = {k.strip('_'): (columns[k](row[k])
                                        #if not isinstance(columns[k], type) or not issubclass(columns[k], linefunc)
                                        if not hasattr(columns[k], 'linefunc')
                                        else columns[k].calc(row))
                         for k in columns.keys()}

        c.execute('INSERT %s INTO slurm (%s) VALUES (%s)'%(
                  'OR REPLACE' if update else '',
                  ','.join('"'+x+'"' for x in processed_row.keys()),
                  ','.join(['?']*len(processed_row))),
            tuple(processed_row.values()))

        # Committing every so often allows other queries to succeed
        if i%10000 == 0:
            #print('committing')
            db.commit()
            if verbose:
                print('... processing row %d'%i)
    db.commit()
    return errors[0]


def args_to_sacct_filter(args, sacct_filter):
    """Generate sacct filter args in a standard way

    For example adding a --completed argument that translates into
    different sacct arguments.
    """
    # A single argument that looks like a jobID is used.
    if len(sacct_filter) == 1 and re.match(r'[0-9+_]+(.[0-9a-z]+)?', sacct_filter[0]):
        sacct_filter = [f'--jobs={sacct_filter[0]}']
    # Set for completed jobs.
    if getattr(args, 'completed', None):
        sacct_filter[:0] = ['--endtime=now', f'--state={COMPLETED_STATES}']
    if getattr(args, 'user', None):
        sacct_filter[:0] = [f'--user={args.user}']
        # Set args.user to None.  We have already handled it here and
        # it shouldn't be re-handled in the future SQL code (future
        # SQL woludn't handle multiple users, for example).
        args.user = None
    if getattr(args, 'partition', None):
        sacct_filter[:0] = [f'--partition={args.partition}']
        args.partition = None
    if getattr(args, 'running_at_time', None):
        sacct_filter[:0] = [f'--start={args.running_at_time}', f'--end={args.running_at_time}', '--state=RUNNING' ]
        args.running_at_time = None
    return sacct_filter

def args_to_sql_where(args):
    where = [ ]
    if getattr(args, 'user', None):
        where.append('and user=:user')
    if getattr(args, 'partition', None):
        where.append("and Partition like '%'||:partition||'%'")
    return ' '.join(where)


def import_or_open_db(args, sacct_filter, csv_input=None):
    """Helper function to either open a DB or generate a new in-mem one from sacct

    The `args` sholud be an argparse argument option.  This function
    will look at its arguments and do what it says.  So, if you want
    various features, you need to define these arguments in argparse:

    db: filename of a database to open

    """
    if args.db:
        db = sqlite3.connect(args.db)
        if sacct_filter:
            LOG.warn("Warning: reading from database.  Any sacct filters are ignored.")
    else:
        # Import fresh
        sacct_filter = args_to_sacct_filter(args, sacct_filter)
        LOG.debug(f'sacct args: {sacct_filter}')
        db = sqlite3.connect(':memory:')
        errors = slurm2sql(db, sacct_filter=sacct_filter,
                           csv_input=getattr(args, 'csv_input', False) or csv_input)
    return db


def update_last_timestamp(db, update_time=None):
    """Update the last update time in the database, for resuming.

    Updates the one row of the meta_slurm_lastupdate with the latest
    unix timestamp, as passed as an argument (or now)
    """
    if update_time is None:
        update_time = time.time()
    if isinstance(update_time, datetime.datetime):
        update_time = datetime_timestamp(update_time)
    update_time = min(update_time, time.time())
    db.execute("INSERT OR REPLACE INTO meta_slurm_lastupdate (id, update_time) VALUES (0, ?)", (update_time, ))
    db.commit()

def get_last_timestamp(db):
    """Return the last update timestamp from the database"""
    return db.execute('SELECT update_time FROM meta_slurm_lastupdate').fetchone()[0]


def slurm_version(cmd=['sacct', '--version']):
    """Return the version number of Slurm, as a tuple"""
    # example output: b'slurm 18.08.8\n' or slurm 19.05.7-Bull.1.0
    try:
        slurm_version = subprocess.check_output(cmd).decode()
    except FileNotFoundError:  # no sacct
        return (20, 11)  # lastest with a schema change
    slurm_version = re.match(r"slurm\s([0-9]+)\.([0-9]+)\.([0-9]+)", slurm_version)
    slurm_version = tuple(int(x) for x in slurm_version.groups())
    return slurm_version




def compact_table():
    """Compact display format.  Function to not depend on tabulate in main body."""
    import tabulate
    from tabulate import Line, DataRow, TableFormat
    tabulate.MIN_PADDING = 0
    return TableFormat(
            lineabove=Line(" ", "-", " ", " "),
            linebelowheader=Line(" ", "-", " ", " "),
            linebetweenrows=None,
            linebelow=None,
            headerrow=DataRow(" ", " ", " "),
            datarow=DataRow(" ", " ", " "),
            padding=0,  # Changed to 0 from 1
            with_header_hide=["lineabove"],
        )


SACCT_DEFAULT_FIELDS = "JobID,User,State,datetime(Start, 'unixepoch') AS Start,datetime(End, 'unixepoch') AS End,Partition,ExitCodeRaw,NodeList,NCPUS,CPUtime,CPUEff,AllocMem,TotalMem,MemEff,ReqGPUS,GPUEff,TotDiskRead,TotDiskWrite,ReqTRES,AllocTRES,TRESUsageInTot,TRESUsageOutTot"
SACCT_DEFAULT_FIELDS_LONG = "JobID,User,State,datetime(Start, 'unixepoch') AS Start,datetime(End, 'unixepoch') AS End,Elapsed,Partition,ExitCodeRaw,NodeList,NCPUS,CPUtime,CPUEff,AllocMem,TotalMem,MemEff,ReqMem,MaxRSS,ReqGPUS,GPUEff,GPUUtil,TotDiskRead,TotDiskWrite,ReqTRES,AllocTRES,TRESUsageInTot,TRESUsageOutTot"
COMPLETED_STATES = 'CA,CD,DL,F,NF,OOM,PR,RV,TO'
def sacct_cli(argv=sys.argv[1:], csv_input=None):
    """A command line that uses slurm2sql to give an sacct-like interface."""
    parser = argparse.ArgumentParser(description=
        "All unknown arguments get passed to sacct to fetch data."
        "For example, one would usually give '-a' or '-S 2019-08-01' here, for example")
    #parser.add_argument('db', help="Database filename to create or update")
    #parser.add_argument('sacct_filter', nargs='*',
    #                    help="sacct options to filter jobs.  For example, one "
    #                         "would usually give '-a' or '-S 2019-08-01' "
    #                         "here, for example")
    parser.add_argument('--db',
                        help="Read from this DB.  Don't import new data.")
    parser.add_argument('--output', '-o', default=SACCT_DEFAULT_FIELDS,
                        help="Fields to output (comma separated list, use '*' for all fields).  NOT safe from SQL injection.  If 'long' then some longer default list")
    parser.add_argument('--format', '-f', default=compact_table(),
                        help="Output format (see tabulate formats: https://pypi.org/project/tabulate/ (default simple)")
    parser.add_argument('--order',
                        help="SQL order by (arbitrary SQL expression using column names).  NOT safe from SQL injection.")
    parser.add_argument('--csv-input',
                        help="Don't parse sacct but import this CSV file.  It's read with "
                             "Python's default csv reader (excel format).  Beware badly "
                             "formatted inputs, for example line breaks in job names.")
    parser.add_argument('--quiet', '-q', action='store_true',
                        help="Don't output anything unless errors")
    parser.add_argument('--verbose', '-v', action='store_true',
                        help="Output more logging info")
    # No --db compatibility
    group = parser.add_argument_group(description="Selectors that only works when getting new data (not with --db):")
    group.add_argument('--completed', '-c', action='store_true',
                        help=f"Select for completed job states ({COMPLETED_STATES})  You need to specify --starttime (-S) at some point in the past, due to how saccont default works (for example '-S now-1week').  This option automatically sets '-E now'.  Not compatible with --db.")
    group.add_argument('--running-at-time', metavar='TIME', help="Only jobs running at this time.  Not compatible with --db.  Expanded to --start=TIME --end=TIME --state=R.")
    # --db compatibility
    group = parser.add_argument_group(description="Selectors that also work with --db:")
    group.add_argument('--user', '-u', help="Limit to this or these users.  Compatible with --db.")
    group.add_argument('--partition', '-r', help="Jobs in this partition.  Works with --db.  Getting fresh data, an exact match and can be a comma separated list.  With --db, a raw glob match.")

    args, sacct_filter = parser.parse_known_args(argv)

    if args.verbose:
        logging.lastResort.setLevel(logging.DEBUG)
    if args.quiet:
        logging.lastResort.setLevel(logging.WARN)
    LOG.debug(args)
    if args.output == 'long':
        args.output = SACCT_DEFAULT_FIELDS_LONG

    db = import_or_open_db(args, sacct_filter, csv_input=csv_input)

    # If we run sacct, then args.user is set to None so we don't do double filtering here
    where = args_to_sql_where(args)

    from tabulate import tabulate
    cur = db.execute(f'select {args.output} from slurm WHERE true {where}',
                     {'user':args.user, 'partition': args.partition})
    headers = [ x[0] for x in cur.description ]
    print(tabulate(cur, headers=headers, tablefmt=args.format))


def seff_cli(argv=sys.argv[1:], csv_input=None):
    parser = argparse.ArgumentParser(usage=
        "slurm2sql-seff [-h] [--order ORDER] [--completed --starttime TIME] SACCT_ARGS",
        description=
        """Print out efficiency of different jobs.  Included is CPU,
        memory, GPU, and i/o stats.

        All extra arguments get passed to `sacct` to fetch data job
        data.  For example, one would usually give '-a' or '-S
        2019-08-01' here, for example.  To look only at completed
        jobs, use "--completed -S now-1week" (a start time must be
        given with --completed because of how sacct works).

        This only queries jobs with an End time (unlike most other commands).

        If a single argument is given, and it
        looks like a JobID, then use only on that single job with
        --jobs=[JobID].""")
    #parser.add_argument('db', help="Database filename to create or update")
    #parser.add_argument('sacct_filter', nargs=0,
    #                    help="sacct options to filter jobs.  )
    parser.add_argument('--db',
                        help="Read from this DB.  Don't import new data.")
    parser.add_argument('--format', '-f', default=compact_table(),
                        help="Output format (see tabulate formats: https://pypi.org/project/tabulate/ (default simple)")
    parser.add_argument('--aggregate-user', action='store_true',
                        help="Aggregate data by user.")
    parser.add_argument('--order',
                        help="SQL order by (arbitrary SQL expression using column names).  NOT safe from SQL injection.")
    parser.add_argument('--csv-input',
                        help="Don't parse sacct but import this CSV file.  It's read with "
                             "Python's default csv reader (excel format).  Beware badly "
                             "formatted inputs, for example line breaks in job names.")
    parser.add_argument('--quiet', '-q', action='store_true',
                        help="Don't output anything unless errors")
    parser.add_argument('--verbose', '-v', action='store_true',
                        help="Output more logging info")
    # No --db compatibility
    group = parser.add_argument_group(description="Selectors that only works when getting new data (not with --db):")
    group.add_argument('--completed', '-c', action='store_true',
                        help=f"Select for completed job states ({COMPLETED_STATES})  You need to specify --starttime (-S) at some point in the past, due to how saccont default works (for example '-S now-1week').  This option automatically sets '-E now'.  Not compatible with --db.")
    group.add_argument('--running-at-time', metavar='TIME', help="Only jobs running at this time.  Not compatible with --db.  Expanded to --start=TIME --end=TIME --state=R.")
    # --db compatibility
    group = parser.add_argument_group(description="Selectors that also work with --db:")
    group.add_argument('--user', '-u', help="Limit to this or these users.  Compatible with --db.")
    group.add_argument('--partition', '-r', help="Jobs in this partition.  Works with --db.  Getting fresh data, an exact match and can be a comma separated list.  With --db, a raw glob match.")

    args, sacct_filter = parser.parse_known_args(argv)

    if args.verbose:
        logging.lastResort.setLevel(logging.DEBUG)
    if args.quiet:
        logging.lastResort.setLevel(logging.WARN)
    LOG.debug(args)

    if args.order:
        order_by = f'ORDER BY {args.order}'
    else:
        order_by = ''

    db = import_or_open_db(args, sacct_filter, csv_input=csv_input)

    # If we run sacct, then args.user is set to None so we don't do double filtering here
    where = args_to_sql_where(args)

    from tabulate import tabulate

    if args.aggregate_user:
        cur = db.execute(f"""select * from ( select
                                User,
                                round(sum(Elapsed)/86400,1) AS days,

                                round(sum(Elapsed*NCPUS)/86400,1) AS cpu_day,
                                printf("%2.0f%%", 100*sum(Elapsed*NCPUS*CPUEff)/sum(Elapsed*NCPUS)) AS CPUEff,

                                round(sum(Elapsed*AllocMem)/1073741824/86400,1) AS mem_GiB_day,
                                printf("%2.0f%%", 100*sum(Elapsed*AllocMem*MemEff)/sum(Elapsed*AllocMem)) AS MemEff,

                                round(sum(Elapsed*NGPUs)/86400,1) AS gpu_day,
                                iif(sum(NGpus), printf("%2.0f%%", 100*sum(Elapsed*NGPUs*GPUeff)/sum(Elapsed*NGPUs)), NULL) AS GPUEff,

                                round(sum(TotDiskRead/1048576)/sum(Elapsed),2) AS read_MiBps,
                                round(sum(TotDiskWrite/1048576)/sum(Elapsed),2) AS write_MiBps

                                FROM eff
                                WHERE End IS NOT NULL {where}
                            GROUP BY user ) {order_by}
                            """, {'user': args.user, 'partition': args.partition})
        headers = [ x[0] for x in cur.description ]
        data = cur.fetchall()
        if len(data) == 0:
            print("No data fetched with these sacct options.")
            exit(2)
        print(tabulate(data, headers=headers, tablefmt=args.format, colalign=('left', 'decimal',)+('decimal', 'right')*3))
        sys.exit()

    cur = db.execute(f"""select * from ( select
                         JobID,
                         User,
                         round(Elapsed/3600,2) AS hours,

                         NCPUS,
                         printf("%3.0f%%",round(CPUeff, 2)*100) AS "CPUeff",

                         round(AllocMem/1073741824,2) AS MemAllocGiB,
                         round(TotalMem/1073741824,2) AS MemTotGiB,
                         printf("%3.0f%%",round(MemEff,2)*100)  AS MemEff,

                         NGpus,
                         iif(NGpus, printf("%3.0f%%",round(GPUeff,2)*100), NULL) AS GPUeff,
                         iif(NGpus, printf("%4.1f",GPUmem/1073741824), NULL) AS GPUmemGiB,

                         round(TotDiskRead/Elapsed/1048576,2) AS read_MiBps,
                         round(TotDiskWrite/Elapsed/1048576,2) AS write_MiBps

                         FROM eff
                         WHERE End IS NOT NULL {where} ) {order_by}""", {'user': args.user, 'partition': args.partition})
    headers = [ x[0] for x in cur.description ]
    data = cur.fetchall()
    if len(data) == 0:
        print("No data fetched with these sacct options.")
        exit(2)
    print(tabulate(data, headers=headers, tablefmt=args.format,
                       colalign=('decimal', 'left', 'decimal',
                                 'decimal', 'right', # cpu
                                 'decimal', 'decimal', 'right', # mem
                                 'decimal', 'right', # gpu
                                 'decimal', 'decimal', # io
                                )))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'sacct':
        exit(sacct_cli(sys.argv[2:]))
    if len(sys.argv) > 1 and sys.argv[1] == 'seff':
        exit(seff_cli(sys.argv[2:]))
    exit(main(sys.argv[1:]))
