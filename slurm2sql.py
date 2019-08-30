#!/usr/bin/env python3

# pylint: disable=too-few-public-methods, missing-docstring

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

LOG = logging.getLogger('slurm2sql')
LOG.setLevel(logging.DEBUG)
if sys.version_info[0] >= 3:
    logging.lastResort.setLevel(logging.INFO)


#
# First, many converter functions/classes which convert strings to
# useful values.
#

# Single converter functions: transform one column to sqlite value
# stored as that same column.
def nullint(x):
    """int or None"""
    return int(x) if x else None

def nullstr_strip(x):
    """str or None"""
    return str(x).strip() if x else None

def timestamp(x):
    """Timestamp in local time, converted to unixtime"""
    if not x:           return None
    if x == 'Unknown':  return None
    return time.mktime(time.strptime(x, '%Y-%m-%dT%H:%M:%S'))

def slurmtime(x):
    """Parse slurm time of format [dd-[hh:]]mm:ss"""
    if not x: return None
    seconds = 0
    if '-' in x:
        days, time_ = x.split('-', 1)
        seconds += int(days) * 24 * 3600
    else:
        time_ = x
    hms = time_.split(':')
    if len(hms) >= 3:   seconds += int(hms[-3])*3600
    if len(hms) >= 2:   seconds += int(hms[-2])*60
    if len(hms) >= 1:   seconds += float(hms[-1])
    return seconds

def str_unknown(x):
    if x == 'Unknown': return None
    return x

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

def float_bytes(x, convert=float):
    """Convert a float with unit (K,M, etc) to value"""
    if not x:  return None
    unit = x[-1].lower()
    if unit in 'kmgtpezy':
        return convert(x[:-1]) * unit_value_binary(unit)
    return convert(x)

def int_bytes(x):
    return float_bytes(x, convert=lambda x: int(float(x)))

def float_metric(x, convert=float):
    """Convert a float with unit (K,M, etc) to value"""
    if not x:  return None
    unit = x[-1].lower()
    if unit in 'kmgtpezy':
        return convert(x[:-1]) * unit_value_metric(unit)
    return convert(x)

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

# Submit, start, and end times as unixtimes
class slurmSubmitTS(linefunc):
    @staticmethod
    def calc(row):
        return timestamp(row['Submit'])

class slurmStartTS(linefunc):
    @staticmethod
    def calc(row):
        return timestamp(row['Start'])

class slurmEndTS(linefunc):
    @staticmethod
    def calc(row):
        return timestamp(row['End'])

# Memory stuff
class slurmMemNode(linefunc):
    """Memory per node"""
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if not reqmem:  return None
        ncpus = int(row['NCPUS'])
        if ncpus == 0:  return 0
        nnodes = int(row['NNodes'])
        if nnodes == 0: return None
        if reqmem.endswith('c'):
            return slurmmem(reqmem) * ncpus / nnodes
        if reqmem.endswith('n'):
            return slurmmem(reqmem)

class slurmMemCPU(linefunc):
    """Memory per cpu, computed if necessary"""
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if not reqmem:  return None
        nnodes = int(row['NNodes'])
        if nnodes == 0: return 0
        ncpus = int(row['NCPUS'])
        if ncpus == 0:  return None
        if reqmem.endswith('c'):
            return slurmmem(reqmem)
        if reqmem.endswith('n'):
            return slurmmem(reqmem) * nnodes / ncpus

class slurmMemType(linefunc):
    """Memory type: 'n' per node, 'c' per core"""
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if not reqmem: return None
        if reqmem.endswith('n'):  return 'n'
        if reqmem.endswith('c'):  return 'c'
        raise ValueError("Unknown memory type")

class slurmMemRaw(linefunc):
    """Raw value of ReqMem column, with 'c' or 'n' suffix"""
    @staticmethod
    def calc(row):
        return row['ReqMem']

# GPU stuff
class slurmReqGPU(linefunc):
    @staticmethod
    def calc(row):
        gres = row['ReqGRES']
        if not gres:  return None
        m = re.search(r'gpu:(\d+)', gres)
        if m:
            return int(m.group(1))

class slurmGPUMem(linefunc):
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip():  return
        if 'No GPU stats' in comment:  return
        comment = json.loads(comment)
        if 'gpu_mem_max' not in comment:  return
        return comment.get('gpu_mem_max') * (2**20)

class slurmGPUUtil(linefunc):
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip(): return
        if 'No GPU stats' in comment:  return
        comment = json.loads(comment)
        if 'gpu_util' not in comment:  return
        return comment['gpu_util']/100.

class slurmGPUCount(linefunc):
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip():        return
        if 'No GPU stats' in comment:  return
        comment = json.loads(comment)
        return comment.get('ngpu')

# Job ID related stuff
class slurmJobIDParent(linefunc):
    """The JobID without any . or _"""
    @staticmethod
    def calc(row):
        return int(row['JobID'].split('_')[0].split('.')[0])

class slurmArrayID(linefunc):
    @staticmethod
    def calc(row):
        if '_' not in row['JobID']:  return
        if '[' in row['JobID']:      return
        return int(row['JobID'].split('_')[1].split('.')[0])

class slurmStepID(linefunc):
    @staticmethod
    def calc(row):
        if '.' not in row['JobID']:  return
        return row['JobID'].split('.')[-1]

# Efficiency stuff
class slurmMemEff(linefunc):
    #https://github.com/SchedMD/slurm/blob/master/contribs/seff/seff
    @staticmethod
    def calc(row):
        reqmem_type = slurmMemType.calc(row)
        mem_max = slurmmem(row['MaxRSS'])
        reqmem = slurmmem(row['ReqMem'])
        if not reqmem or mem_max is None:  return
        if reqmem_type == 'c':
            nodemem = reqmem * int(row['NCPUS'])
        elif reqmem_type == 'n':
            nodemem = reqmem
        else:
            raise ValueError('unknown memory type: %s'%reqmem_type)
        return mem_max / nodemem

class slurmCPUEff(linefunc):
    # This matches the seff tool currently:
    # https://github.com/SchedMD/slurm/blob/master/contribs/seff/seff
    @staticmethod
    def calc(row):
        walltime = slurmtime(row['Elapsed'])
        if not walltime: return None
        cpueff = slurmtime(row['TotalCPU']) / (walltime * int(row['NCPUS']))
        return cpueff

class slurmConsumedEnergy(linefunc):
    @staticmethod
    def calc(row):
        if not row['ConsumedEnergyRaw']:  return None
        return int(row['ConsumedEnergyRaw'])

class slurmExitCodeRaw(linefunc):
    @staticmethod
    def calc(row):
        if not row['ExitCode']:  return None
        return row['ExitCode']

class slurmExitCode(linefunc):
    @staticmethod
    def calc(row):
        if not row['ExitCode']:  return None
        return int(row['ExitCode'].split(':')[0])

class slurmExitSignal(linefunc):
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
    'JobID': str,                       # Slurm Job ID or 'Job_Array'
    'JobIDRaw': str,                    # Actual job ID, including of array jobs.
    'JobName': str,                     # Free-form text name of the job
    '_ArrayID': slurmArrayID,           # "Array" component of "Job_Array.Step"
    '_StepID': slurmStepID,             # "Step" component of above
    '_JobIDParent': slurmJobIDParent,   # Just the Job part of "Job_Array" for array jobs
    'User': str,                        # Username
    'Group': str,                       # Group
    'Account': str,                     # Account

    # Times and runtime info
    'State': str,                       # Job state
    'Timelimit': slurmtime,             # Timelimit specified by user
    'Elapsed': slurmtime,               # Walltime of the job
    'Submit': str_unknown,              # Submit time in yyyy-mm-ddThh:mm:ss straight from slurm
    'Start': str_unknown,               # Same, job start time
    'End': str_unknown,                 # Same, job end time
    '_SubmitTS': slurmSubmitTS,         # Same as above three, unixtime
    '_StartTS': slurmStartTS,
    '_EndTS': slurmEndTS,
    'Partition': str,                   # Partition
    '_ExitCodeRaw': slurmExitCodeRaw,   # ExitStatus:Signal
    'ExitCode': slurmExitCode,        # ExitStatus from above, int
    '_ExitSignal': slurmExitSignal,     # Signal from above, int
    'NodeList': str,                    # Node list of jobs
    'Priority': nullint,                # Slurm priority (higher = will run sooner)
    '_ConsumedEnergy': slurmConsumedEnergy,

    # Stuff about number of nodes
    'ReqNodes': int_bytes,              # Requested number of nodes
    'NNodes': nullint,                  # Number of nodes (allocated if ran, requested if not yet)
    'AllocNodes': nullint,              # Number of nodes (allocated, zero if not running yet)

    # Miscelaneous requested resources
    #'ReqTRES': str,
    'ReqGRES': str,                     # Raw GRES string
    'NTasks': nullint,
    #'AllocGRES'
    #'AllocTRES'

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
    'MinCPUNode': str,
    'MinCPUTask': str,

    # Memory related
    'ReqMem': str,                      # Requested mem, value from slurm.  Has a 'c' on 'n' suffix
    '_ReqMemType': slurmMemType,        # 'c' for mem-per-cpu or 'n' for mem-per-node
    '_ReqMemNode': slurmMemNode,        # Mem per node, computed if type 'c'
    '_ReqMemCPU': slurmMemCPU,          # Mem per cpu, computed if type 'n'
    'AveRSS': slurmmem,
    'MaxRSS': slurmmem,
    'MaxRSSNode': str,
    'MaxRSSTask': str,
    'MaxPages': int_metric,
    'MaxVMSize': slurmmem,
    '_MemEff': slurmMemEff,             # Slurm memory efficiency

    # Disk related
    'AveDiskRead': int_bytes,
    'AveDiskWrite': int_bytes,
    'MaxDiskRead': int_bytes,
    'MaxDiskReadNode': str,
    'MaxDiskReadTask': str,
    'MaxDiskWrite': int_bytes,
    'MaxDiskWriteNode': str,
    'MaxDiskWriteTask': str,

    # GPU related
    '_ReqGPUS': slurmReqGPU,            # Number of GPUS requested
    'Comment': nullstr_strip,           # Slurm Comment field (at Aalto used for GPU stats)
    '_GPUMem': slurmGPUMem,             # GPU mem extracted from comment field
    '_GPUUtil': slurmGPUUtil,           # GPU utilization (0.0 to 1.0) extracted from comment field
    '_NGPU': slurmGPUCount,             # Number of GPUs, extracted from comment field
    }
COLUMNS_EXTRA = ['ConsumedEnergyRaw']



def main(argv, db=None, lines=None):
    """Parse arguments and use the other API"""
    parser = argparse.ArgumentParser()
    parser.add_argument('db', help="Database filename to create or update")
    parser.add_argument('sacct_filter', nargs='*',
                        help="sacct options to filter jobs.  For example, one "
                             "would usually give '-a' or '-S 2019-08-01' "
                             "here, for example")
    parser.add_argument('--update', '-u', action='store_true',
                        help="If given, don't delete existing database and "
                             "instead insert or update rows")
    parser.add_argument('--history-days', type=int,
                        help="Day-by-day collect history, starting this many days ago.")
    parser.add_argument('--history-start',
                        help="Day-by-day collect history, starting on this day.")
    parser.add_argument('--jobs-only', action='store_true',
                        help="Don't include job steps but only the man jobs")
    args = parser.parse_args(argv)

    # db is only given as an argument in tests (normally)
    if db is None:
        # Delete existing database unless --update/-u is given
        if not args.update and os.path.exists(args.db):
            os.unlink(args.db)
        db = sqlite3.connect(args.db)

    sacct_filter = lines or args.sacct_filter

    # If --history-days, get just this many days history
    if (args.history_days is not None
        or args.history_start is not None):
        errors = get_history(db, sacct_filter=sacct_filter,
                            history_days=args.history_days,
                            history_start=args.history_start,
                            jobs_only=args.jobs_only)

        create_indexes(db)
    # Normal operation
    else:
        errors = slurm2sql(db, sacct_filter=sacct_filter,
                           update=args.update,
                           jobs_only=args.jobs_only)
        create_indexes(db)

    if errors:
        LOG.warning("Completed with %s errors", errors)
        return(1)
    return(0)


def get_history(db, history_days=None, history_start=None, sacct_filter=['-a'],
                jobs_only=False):
    """Get history for a certain period of days.

    Queries each day and updates the database, so as to avoid
    overloading sacct and causing a failure.

    Returns: the number of errors.
    """
    errors = 0
    today = datetime.date.today()
    if history_days:
        start = today - datetime.timedelta(days=history_days)
    elif history_start:
        start = datetime.datetime.strptime(history_start, '%Y-%m-%d').date()

    days_ago = (today - start).days
    day_interval = 1
    while start <= today:
        end = start+datetime.timedelta(days=day_interval)
        new_filter = sacct_filter + [
            '-S', start.strftime('%Y-%m-%d'),
            '-E', end.strftime('%Y-%m-%d'),
            ]
        LOG.info("%s %s", days_ago, start)
        errors += slurm2sql(db, sacct_filter=new_filter, update=True, jobs_only=jobs_only)
        db.commit()
        start = end
        days_ago -= day_interval
    return errors


def sacct(slurm_cols, sacct_filter):
    cmd = ['sacct', '-o', ','.join(slurm_cols), '-P', '--units=K',
           '--delimiter=;|;',
           #'--allocations',  # no job steps, only total jobs, but doesn't show used resources.
           ] + list(sacct_filter)
    #LOG.debug(' '.join(cmd))
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE, universal_newlines=True,
                         errors='replace')
    return p.stdout


def create_indexes(db):
    db.execute('CREATE INDEX IF NOT EXISTS idx_slurm_start_user ON slurm (Start, User)')
    db.execute('ANALYZE;')
    db.commit()


def slurm2sql(db, sacct_filter=['-a'], update=False, jobs_only=False):
    """Import one call of sacct to a sqlite database.

    db:
    open sqlite3 database file object.

    sacct_filter:
    filter for sacct, list of arguments.  This should only be row
    filters, such as ['-a'], ['-S' '2019-08-01'], and so on.  The
    argument should be a list.  You can't currently filter what columns
    are selected.

    Returns: the number of errors
    """
    create_columns = ', '.join('"'+c.strip('_')+'"' for c in COLUMNS)
    create_columns = create_columns.replace('JobIDRaw"', 'JobIDRaw" UNIQUE')
    db.execute('CREATE TABLE IF NOT EXISTS slurm (%s)'%create_columns)
    db.execute('CREATE VIEW IF NOT EXISTS allocations AS select * from slurm where StepID is null;')
    db.execute('PRAGMA journal_mode = WAL;')
    db.commit()
    c = db.cursor()

    slurm_cols = tuple(c for c in list(COLUMNS.keys()) + COLUMNS_EXTRA if not c.startswith('_'))

    # Read data from sacct, or interpert sacct_filter directly as
    # testdata if it has the attribute 'testdata'
    if not hasattr(sacct_filter, 'readlines'):
        # This is a real filter, read data
        lines = sacct(slurm_cols, sacct_filter)
    else:
        # Support tests - raw lines can be put in
        lines = sacct_filter

    # We don't use the csv module because the csv can be malformed.
    # In particular, job name can include newlines(!).  TODO: handle job
    # names with newlines.
    errors = 0
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
            LOG.error("Line with wrong number of columns: (want=%s, have=%s) %s", len(slurm_cols), len(line), rawline)
            errors += 1
            continue
        line = dict(zip(header, line))

        # If --jobs-only, then skip all job steps (sacct updates the
        # mem/cpu usage on the allocation itself already)
        step_id = slurmStepID.calc(line)
        if jobs_only and step_id is not None:
            continue

        #LOG.debug(line)
        processed_line = {k.strip('_'): (COLUMNS[k](line[k])
                                         #if not isinstance(COLUMNS[k], type) or not issubclass(COLUMNS[k], linefunc)
                                         if not hasattr(COLUMNS[k], 'linefunc')
                                         else COLUMNS[k].calc(line))
                          for k in COLUMNS.keys()}

        c.execute('INSERT %s INTO slurm (%s) VALUES (%s)'%(
                  'OR REPLACE' if update else '',
                  ','.join('"'+x+'"' for x in processed_line.keys()),
                  ','.join(['?']*len(processed_line))),
            tuple(processed_line.values()))


        # Committing every so often allows other queries to succeed
        if i%10000 == 0:
            #print('committing')
            db.commit()
    db.commit()
    return errors


if __name__ == "__main__":
    exit(main(sys.argv[1:]))
