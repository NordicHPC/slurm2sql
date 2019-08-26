import argparse
import json
import os
import re
import sqlite3
import subprocess

def nullint(x):
    """int or None"""
    return int(x) if x else None

def nullstr(x):
    """str or None"""
    return str(x) if x else None

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
    x = x.strip('Knc')
    return float(x)//1024

class linefunc(object):
    linefunc = True
class slurmMemNode(linefunc):
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if not reqmem:  return None
        if reqmem.endswith('c'):
            return slurmmem(reqmem) * int(row['NCPUS']) / int(row['NNodes'])
        if reqmem.endswith('n'):
            return slurmmem(reqmem)
class slurmMemType(linefunc):
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if not reqmem: return None
        if reqmem.endswith('n'):  return 'n'
        if reqmem.endswith('c'):  return 'c'
        raise ValueError("Unknown memory type")
class slurmMemRaw(linefunc):
    @staticmethod
    def calc(row):
        return row['ReqMem']
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
        if not comment.strip():  return
        if 'No GPU stats' in comment:  return
        comment = json.loads(comment)
        return comment['ngpu']

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
    #https://github.com/SchedMD/slurm/blob/master/contribs/seff/seff
    @staticmethod
    def calc(row):
        elapsed = slurmtime(row['Elapsed'])
        if not elapsed: return None
        cpueff = slurmtime(row['TotalCPU']) / (elapsed * int(row['NCPUS']))
        return cpueff

COLUMNS = {
    'JobID': str,
    'JobIDRaw': str,
    'JobName': str,
    'User': str,
    'Start': str,
    'End': str,

    'Timelimit': slurmtime,
    'State': str,
    'Elapsed': slurmtime,
    'Partition': str,
    #'ReqTRES': str,
    'ReqGRES': str,
    #'Comment'
    'NTasks': nullint,
    '_ReqGPU': slurmReqGPU,

    #'AllocGRES'
    #'AllocTRES'

    'NNodes': nullint,
    'AllocNodes': nullint,

    'NCPUS': nullint,
    'CPUTime': slurmtime,   # = Elapsed * NCPU    (= CPUTimeRaw)  (not how much used)
    'TotalCPU': slurmtime,  # = Elapsed * NCPU * efficiency
    'UserCPU': slurmtime,
    'SystemCPU': slurmtime,
    'AllocCPUS': nullint,
    '_CPUEff': slurmCPUEff,

    'ReqMem': slurmMemNode,
    '_ReqMemType': slurmMemType,
    '_ReqMemRaw': slurmMemRaw,
    'MaxRSS': slurmmem,
    'AveRSS': slurmmem,

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
    for i, line in enumerate(p.stdout):
        if i == 0:  continue
        line = line.split('|')
        if len(line) < len(slurm_cols):
            continue
        line = dict(zip(slurm_cols, line))

        #print(line)
        processed_line = {k.strip('_'): (COLUMNS[k](line[k])
                                         #if not isinstance(COLUMNS[k], type) or not issubclass(COLUMNS[k], linefunc)
                                         if not hasattr(COLUMNS[k], 'linefunc')
                                         else COLUMNS[k].calc(line))
                          for k in COLUMNS.keys()}
        #print(processed_line)
        c.execute('INSERT INTO slurm (%s) VALUES (%s)'%(
            ','.join(processed_line.keys()),
            ','.join(['?']*len(processed_line))),
            tuple(processed_line.values()))
    db.commit()

