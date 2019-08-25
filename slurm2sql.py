import argparse
import csv
import os
import sqlite3
import subprocess
import sys

def slurmtime(x):
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
    x = x.strip('Knc')
    if x == '': return None
    return float(x)//1024

class linefunc(object):
    linefunc = True
class slurmMemNode(linefunc):
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if reqmem.endswith('c'):
            return slurmmem(reqmem) * int(row['NCPUS']) / int(row('NNodes'))
        if reqmem.endswith('n'):
            return slurmmem(reqmem)
class slurmMemType(linefunc):
    @staticmethod
    def calc(row):
        reqmem = row['ReqMem']
        if reqmem.endswith('n'): return 'n'
        if reqmem.endswith('c'): return 'c'
        raise ValueError("Unknown memory type")
class slurmMemRaw(linefunc):
    @staticmethod
    def calc(row):
        return row['ReqMem']


COLUMNS = {
    'JobID': str,
    'JobIDRaw': str,
    'JobName': str,
    'User': str,
    'Start': str,
    'End': str,

    'TimeLimit': slurmtime,
    #'ReqTRES': str,
    #'ReqGRES':
    #'Comment'
    #'NTasks'

    #'AllocGRES'
    #'AllocTRES'

    'NCPUS': int,
    'NNodes': int,

    'AllocCPUS': int,
    'CPUTime': slurmtime,   # = CPUTimeRaw
    'TotalCPU': slurmtime,

    'ReqMem': slurmMemNode,
    '_ReqMemType': slurmMemType,
    '_ReqMemRaw': slurmMemRaw,
    'MaxRSS': slurmmem,
    'AveRSS': slurmmem,
    'State': str,
    'Elapsed': slurmtime,
    'Partition': str,
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
    c = db.cursor()

    cols = ','.join(c for c in COLUMNS.keys() if not c.startswith('_'))
    p = subprocess.Popen(['sacct', '-o', cols, '-P', '--units=K',
                          *args.sacct_filter],
                         stdout=subprocess.PIPE, universal_newlines=True)

    for line in csv.DictReader(p.stdout, delimiter='|'):
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

