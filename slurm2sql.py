#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import re
import sqlite3
import subprocess

# Single converter functions: transform one column to sqlite value
# stored as that same column.
def nullint(x):
    """int or None"""
    return int(x) if x else None

def nullstr(x):
    """str or None"""
    return str(x) if x else None

def timestamp(x):
    """Timestamp in local time, converted to unixtime"""
    if not x:           return None
    if x == 'Unknown':  return None
    return datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S').timestamp()

def slurmtime(x):
    """Parse slurm time of format [dd-[hh:]]mm:ss"""
    if not x: return None
    seconds = 0
    if '-' in x:
        days, time = x.split('-', 1)
        seconds += int(days) * 24 * 3600
    else:
        time = x
    hms = time.split(':')
    if len(hms) >= 3:   seconds += int(hms[-3])*3600
    if len(hms) >= 2:   seconds += int(hms[-2])*60
    if len(hms) >= 1:   seconds += float(hms[-1])
    return seconds

def slurmmem(x):
    """Memory, removing 'n' or 'c' at end, in KB"""
    if not x:  return None
    x = x.strip('nc')
    return float_bytes(x)

# Converting kibi/mibi, etc units to numbers
def unit_value(unit):
    """Convert a unit to its value, e.g. 'K'-->1024, 'M'-->1048576"""
    if unit is None: unit = '_'
    return 2**(10*'_kmgtpezy'.index(unit.lower()))

def float_bytes(x, convert=float):
    """Convert a float with unit (K,M, etc) to value"""
    if not x:  return None
    unit = x[-1].lower()
    if unit in 'kmgtpezy':
        val = x[:-1]
    else:
        unit = '_'
        val = x
    return convert(val) * unit_value(unit)

def int_bytes(x):
    return float_bytes(x, convert=lambda x: int(float(x)))

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
        if reqmem.endswith('c'):
            return slurmmem(reqmem) * int(row['NCPUS']) / int(row['NNodes'])
        if reqmem.endswith('n'):
            return slurmmem(reqmem)

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
        m = re.search('gpu:(\d+)', gres)
        if m:
            return int(m.group(1))

class slurmGPUMem(linefunc):
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip():  return
        if 'No GPU stats' in comment:  return
        comment = json.loads(comment)
        return comment['gpu_mem_max']

class slurmGPUUtil(linefunc):
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip(): return
        if 'No GPU stats' in comment:  return
        comment = json.loads(comment)
        return comment['gpu_util']/100.

class slurmGPUCount(linefunc):
    @staticmethod
    def calc(row):
        comment = row['Comment']
        if not comment.strip():        return
        if 'No GPU stats' in comment:  return
        comment = json.loads(comment)
        return comment['ngpu']

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
#class slurmMemEff(linefunc):
#    #https://github.com/SchedMD/slurm/blob/master/contribs/seff/seff
#    @staticmethod
#    def calc(row):
#        walltime = slurmtime(row['Elapsed'])
#        reqmemtype = slurmMemType(row)
#        reqmem = int(row['ReqMem'].strip('Knc'))
#        if reqmemtype == 'c':
#            reqmem = reqmem * int(row['NCPUS'])
#            pernode = False
#        else:
#            reqmem = reqmem * int(row['NNodes'])
#            pernode = True

class slurmCPUEff(linefunc):
    # This matches the seff tool currently:
    # https://github.com/SchedMD/slurm/blob/master/contribs/seff/seff
    @staticmethod
    def calc(row):
        walltime = slurmtime(row['Elapsed'])
        if not walltime: return None
        cpueff = slurmtime(row['TotalCPU']) / (walltime * int(row['AllocCPUS']))
        return cpueff



# All defined columns and their respective converter functions.
# If it begins in a underscore, this is not a Slurm DB field, it is
# computed.  Don't get it from sacct, but add it to our database
# without the underscore.
COLUMNS = {
    # Basic job metadata
    'JobID': str,
    'JobIDRaw': str,
    'JobName': str,
    '_ArrayID': slurmArrayID,
    '_StepID': slurmStepID,
    '_JobIDParent': slurmJobIDParent,
    'User': str,
    'Account': str,

    # Time limit and runtime info
    'State': str,
    'Timelimit': slurmtime,
    'Elapsed': slurmtime,
    'Submit': str,
    '_SubmitTS': slurmSubmitTS,
    'Start': str,
    'End': str,
    '_StartTS': slurmStartTS,
    '_EndTS': slurmEndTS,
    'Partition': str,
    'ExitCode': str,
    'NodeList': str,
    'ReqNodes': int_bytes,
    'Priority': nullint,

    # Miscelaneous requested resources
    #'ReqTRES': str,
    'ReqGRES': str,
    'NTasks': nullint,
    '_ReqGPU': slurmReqGPU,
    'NNodes': nullint,
    'AllocNodes': nullint,
    #'AllocGRES'
    #'AllocTRES'

    # CPU related
    'NCPUS': nullint,       # = AllocCPUS
    'ReqCPUS': nullint,
    'AllocCPUS': nullint,
    'CPUTime': slurmtime,   # = Elapsed * NCPUS    (= CPUTimeRaw)  (not how much used)
    'TotalCPU': slurmtime,  # = Elapsed * NCPUS * efficiency
    'UserCPU': slurmtime,
    'SystemCPU': slurmtime,
    '_CPUEff': slurmCPUEff,
    'MinCPU': slurmtime,
    'MinCPUNode': str,
    'MinCPUTask': str,

    # Memory related
    'ReqMem': slurmMemNode,
    '_ReqMemType': slurmMemType,
    '_ReqMemRaw': slurmMemRaw,
    'AveRSS': slurmmem,
    'MaxRSS': slurmmem,
    'MaxRSSNode': str,
    'MaxRSSTask': str,

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
    'Comment': nullstr,
    '_GPUMem': slurmGPUMem,
    '_GPUUtil': slurmGPUUtil,
    '_NGPU': slurmGPUCount,
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--update', '-u', action='store_true')
    parser.add_argument('db')
    parser.add_argument('sacct_filter', nargs='*')
    args = parser.parse_args()

    if not args.update and os.path.exists(args.db):
        os.unlink(args.db)
    db = sqlite3.connect(args.db)
    create_columns = ', '.join(c.strip('_') for c in COLUMNS)
    create_columns = create_columns.replace('JobIDRaw', 'JobIDRaw UNIQUE')
    db.execute('CREATE TABLE IF NOT EXISTS slurm (%s)'%create_columns)
    db.execute('CREATE TABLE IF NOT EXISTS slurm (%s)'%create_columns)
    #db.execute('CREATE VIEW IF NOT EXISTS some_view as select *, (TotalCPU/Elapsed*NCPUS) from slurm;')
    c = db.cursor()

    slurm_cols = tuple(c for c in COLUMNS.keys() if not c.startswith('_'))
    cmd = ['sacct', '-o', ','.join(slurm_cols), '-P', '--units=K',
           #'--allocations',  # no job steps, only total jobs, but doesn't show used resources.
           *args.sacct_filter]
    print(' '.join(cmd))
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE, universal_newlines=True)

    # We don't use the csv module because the csv can be malformed.
    # In particular, job name can include newlines.  TODO: handle job
    # names with newlines.
    for i, rawline in enumerate(p.stdout):
        if i == 0:  continue
        line = rawline.split('|')
        if len(line) < len(slurm_cols):
            continue
        line = dict(zip(slurm_cols, line))

        #print(line)
        processed_line = {k.strip('_'): (COLUMNS[k](line[k])
                                         #if not isinstance(COLUMNS[k], type) or not issubclass(COLUMNS[k], linefunc)
                                         if not hasattr(COLUMNS[k], 'linefunc')
                                         else COLUMNS[k].calc(line))
                          for k in COLUMNS.keys()}

        def insert(processed_line_):
            #print(processed_line)
            c.execute('INSERT %s INTO slurm (%s) VALUES (%s)'%(
                'OR REPLACE' if args.update else '',
                ','.join(processed_line_.keys()),
                ','.join(['?']*len(processed_line_))),
                tuple(processed_line_.values()))

        #last_allocation = processed_line
        #if last_line['JobIDParent'] == processed_line['JobIDParent'] and processed_line['StepID'] == 'batch':
        #    last_allocation['

        insert(processed_line)


    db.commit()

