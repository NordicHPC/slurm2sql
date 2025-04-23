
# pylint: disable=redefined-outer-name
import csv
import datetime
import getpass
from io import StringIO
import os
import sqlite3
import sys
import tempfile
import time

import pytest

import slurm2sql
from slurm2sql import unixtime

has_sacct = os.system('sacct --version') == 0
if sys.version_info[0] <= 2: # python3
    StringIO = lambda s, StringIO=StringIO: StringIO(s.decode())


os.environ['TZ'] = 'Europe/Helsinki'
time.tzset()

#
# Fixtures
#
@pytest.fixture()
def db():
    """Test, in-memory database fixture"""
    with sqlite3.connect(':memory:') as db:
        yield db

@pytest.fixture()
def dbfile():
    """Test, in-memory database fixture"""
    with tempfile.NamedTemporaryFile() as dbfile:
        yield dbfile.name

@pytest.fixture()
def slurm_version(monkeypatch, slurm_version_number=(20, 10)):
    print('Setting Slurm version to %s'%(slurm_version_number,))
    monkeypatch.setattr(slurm2sql, 'slurm_version', lambda: slurm_version_number)
    yield

@pytest.fixture()
def slurm_version_2011(monkeypatch, slurm_version_number=(20, 11, 1)):
    print('Setting Slurm version to %s'%(slurm_version_number,))
    monkeypatch.setattr(slurm2sql, 'slurm_version', lambda: slurm_version_number)
    yield


@pytest.fixture(scope='function')
def data1(slurm_version):
    """Test data set 1"""
    #lines = open('tests/test-data1.csv').read().replace('|', ';|;')
    #yield StringIO(lines)
    yield csv.DictReader(open('tests/test-data1.csv'), delimiter='|')

@pytest.fixture(scope='function')
def data2(slurm_version_2011):
    """Test data set 2.

    This is the same as data1, but removes the ReqGRES column (for slurm>=20.11)
    """
    #lines = open('tests/test-data2.csv').read().replace('|', ';|;')
    #yield StringIO(lines)
    yield csv.DictReader(open('tests/test-data2.csv'), delimiter='|')

@pytest.fixture(scope='function')
def data3(slurm_version_2011):
    """A CSV dataset
    """
    yield 'tests/test-data3.csv'


def csvdata(data):
    """Convert string CSV to a reader for s2s"""
    reader = csv.DictReader(StringIO(data.strip()))
    return reader

def fetch(db, jobid, field, table='slurm'):
    selector = 'JobID'
    if table == 'eff':
        selector = 'JobID'
    r = db.execute(f"SELECT {field} FROM {table} WHERE {selector}=?", (jobid,))
    return r.fetchone()[0]

#
# Tests
#
def test_slurm2sql_basic(db, data1):
    slurm2sql.slurm2sql(db, sacct_filter=[], csv_input=data1)
    r = db.execute("SELECT JobName, Start "
                   "FROM slurm WHERE JobID=43974388;").fetchone()
    assert r[0] == 'spawner-jupyterhub'
    assert r[1] == 1564601354

def test_csv(db, data3):
    slurm2sql.slurm2sql(db, sacct_filter=[], csv_input=data3)
    r = db.execute("SELECT JobName, Start "
                   "FROM slurm WHERE JobID=1;").fetchone()
    print(r)
    assert r[0] == 'job1'
    assert r[1] == 3600

def test_main(db, data1):
    slurm2sql.main(['dummy'], csv_input=data1, db=db)
    r = db.execute("SELECT JobName, Start "
                   "FROM slurm WHERE JobID=43974388;").fetchone()
    assert r[0] == 'spawner-jupyterhub'
    assert r[1] == 1564601354
    assert db.execute("SELECT count(*) from slurm;").fetchone()[0] == 5

def test_jobs_only(db, data1):
    """--jobs-only gives two rows"""
    slurm2sql.main(['dummy', '--jobs-only'], csv_input=data1, db=db)
    assert db.execute("SELECT count(*) from slurm;").fetchone()[0] == 2

def test_verbose(db, data1, caplog):
    slurm2sql.main(['dummy', '--history-days=1', '-v'], csv_input=data1, db=db)
    assert time.strftime("%Y-%m-%d") in caplog.text

def test_quiet(db, data1, caplog, capfd):
    slurm2sql.main(['dummy', '-q'], csv_input=data1, db=db)
    slurm2sql.main(['dummy', '--history=1-5', '-q'], csv_input=data1, db=db)
    slurm2sql.main(['dummy', '--history-days=1', '-q'], csv_input=data1, db=db)
    slurm2sql.main(['dummy', '--history-start=2019-01-01', '-q'], csv_input=data1, db=db)
    #assert caplog.text == ""
    captured = capfd.readouterr()
    assert captured.out == ""
    assert captured.err == ""

def test_time(db, data1):
    slurm2sql.main(['dummy'], csv_input=data1, db=db)
    r = db.execute("SELECT Time FROM slurm WHERE JobID=43974388;").fetchone()[0]
    assert r == unixtime('2019-08-01T02:02:39')
    # Submit defined, Start defined, End='Unknown' --> timestamp should be "now"
    r = db.execute("SELECT Time FROM slurm WHERE JobID=43977780;").fetchone()[0]
    assert r >= time.time() - 5
    # Job step: Submit defined, Start='Unknown', End='Unknown' --> Time should equal Submit
    r = db.execute("SELECT Time FROM slurm WHERE JobID='43977780.batch';").fetchone()[0]
    assert r == unixtime('2019-08-01T00:35:27')

def test_queuetime(db, data1):
    slurm2sql.main(['dummy'], csv_input=data1, db=db)
    r = db.execute("SELECT QueueTime FROM slurm WHERE JobID=43974388;").fetchone()[0]
    assert r == 1

#
# Test different fields
#
def test_cpueff(db):
    data = """
    JobID,CPUTime,TotalCPU
    1,50:00,25:00
    """
    slurm2sql.slurm2sql(db, [], csv_input=csvdata(data))
    print(db.execute('select * from eff;').fetchall())
    assert fetch(db, 1, 'CPUTime') == 3000
    assert fetch(db, 1, 'TotalCPU') == 1500
    assert fetch(db, 1, 'CPUeff', table='eff') == 0.5

def test_gpueff(db):
    data = """
    JobID,AllocTRES,TRESUsageInTot
    1,gres/gpu=1,gres/gpuutil=23
    """
    slurm2sql.slurm2sql(db, [], csv_input=csvdata(data))
    print(db.execute('select * from eff;').fetchall())
    assert fetch(db, 1, 'GpuEff', table='eff') == 0.23


#
# Test command line
#
@pytest.mark.skipif(not has_sacct, reason="Can only be tested with sacct")
def test_cmdline(dbfile):
    ten_days_ago = (datetime.datetime.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    five_days_ago = (datetime.datetime.today() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    os.system('python3 slurm2sql.py %s -- -S %s'%(dbfile, ten_days_ago))
    os.system('python3 slurm2sql.py %s -- -S %s -E %s'%(
        dbfile, ten_days_ago, five_days_ago))
    sqlite3.connect(dbfile).execute('SELECT JobName from slurm;')

@pytest.mark.skipif(not has_sacct, reason="Can only be tested with sacct")
def test_cmdline_history_days(dbfile):
    os.system('python3 slurm2sql.py --history-days=10 %s --'%dbfile)
    sqlite3.connect(dbfile).execute('SELECT JobName from slurm;')

@pytest.mark.skipif(not has_sacct, reason="Can only be tested with sacct")
def test_cmdline_history_start(dbfile):
    ten_days_ago = (datetime.datetime.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    os.system('python3 slurm2sql.py --history-start=%s %s --'%(ten_days_ago, dbfile))
    sqlite3.connect(dbfile).execute('SELECT JobName from slurm;')

@pytest.mark.skipif(not has_sacct, reason="Can only be tested with sacct")
def test_cmdline_history(dbfile):
    print('x')
    os.system('python3 slurm2sql.py --history=2-10 %s --'%dbfile)
    sqlite3.connect(dbfile).execute('SELECT JobName from slurm;')

#
# slurm2sql-sacct
#
def test_sacct(db, capsys):
    data = """
    JobID,CPUTime,TotalCPU
    111,50:00,25:00
    """
    slurm2sql.sacct_cli(argv=[], csv_input=csvdata(data))
    captured = capsys.readouterr()
    assert '111' in captured.out
    assert str(50*60) in captured.out  # cputime

#
# slurm2sql-seff
#
def test_seff(db, capsys):
    data = """
    JobID,End,CPUTime,TotalCPU
    111,1970-01-01T00:00:00,50:00,25:00
    111.2,,,25:00
    """
    slurm2sql.seff_cli(argv=[], csv_input=csvdata(data))
    captured = capsys.readouterr()
    assert '111' in captured.out
    assert '50%' in captured.out

def test_seff_mem(db, capsys):
    data = """
    JobID,End,NNodes,NCPUS,ReqMem,MaxRSS,AllocTRES,TRESUsageInTot
    111,1970-01-01T00:00:00,1,1,10G,,mem=10G,
    111.2,,1,1,,8G,mem=10G,mem=6G
    """
    # Changed 2025-04-23: no longer uses ReqMe.m and MaxRSS but AllocTRES and TRESUsageInTot
    slurm2sql.seff_cli(argv=[], csv_input=csvdata(data))
    captured = capsys.readouterr()
    assert '111' in captured.out
    assert '60%' in captured.out

def test_seff_gpu(db, capsys):
    data = """
    JobID,End,Elapsed,TotalCPU,NCPUS,AllocTRES,TRESUsageInTot
    111,1970-01-01T00:00:00,,1,1,,
    111.2,1970-01-01T00:00:00,100,1,1,gres/gpu=1,gres/gpuutil=23
    """
    slurm2sql.seff_cli(argv=[], csv_input=csvdata(data))
    captured = capsys.readouterr()
    print(captured)
    assert '111' in captured.out
    assert '23%' in captured.out


#
# Misc function tests
#
def test_binary_units():
    assert slurm2sql.int_bytes('2k') == 2048
    assert slurm2sql.int_bytes('2M') == 2 * 2**20
    assert slurm2sql.int_bytes('2G') == 2 * 2**30
    assert slurm2sql.int_bytes('2T') == 2 * 2**40
    assert slurm2sql.int_bytes('2p') == 2 * 2**50
    assert isinstance(slurm2sql.int_bytes('2k'), int)

    assert slurm2sql.float_bytes('2k') == 2048
    assert slurm2sql.float_bytes('2M') == 2 * 2**20
    assert slurm2sql.float_bytes('2G') == 2 * 2**30
    assert slurm2sql.float_bytes('2t') == 2 * 2**40
    assert slurm2sql.float_bytes('2P') == 2 * 2**50
    assert isinstance(slurm2sql.float_bytes('2k'), float)

def test_metric_units():
    assert slurm2sql.int_metric('2k') == 2 * 1000**1
    assert slurm2sql.int_metric('2M') == 2 * 1000**2
    assert slurm2sql.int_metric('2G') == 2 * 1000**3
    assert slurm2sql.int_metric('2T') == 2 * 1000**4
    assert slurm2sql.int_metric('2p') == 2 * 1000**5
    assert isinstance(slurm2sql.int_metric('2k'), int)

    assert slurm2sql.float_metric('2k') == 2 * 1000**1
    assert slurm2sql.float_metric('2M') == 2 * 1000**2
    assert slurm2sql.float_metric('2G') == 2 * 1000**3
    assert slurm2sql.float_metric('2t') == 2 * 1000**4
    assert slurm2sql.float_metric('2P') == 2 * 1000**5
    assert isinstance(slurm2sql.float_metric('2k'), float)

def test_slurm_time():
    assert slurm2sql.slurmtime('1:00:00') == 3600
    assert slurm2sql.slurmtime('1:10:00') == 3600 + 600
    assert slurm2sql.slurmtime('1:00:10') == 3600 + 10
    assert slurm2sql.slurmtime('00:10') == 10
    assert slurm2sql.slurmtime('10:10') == 600 + 10
    assert slurm2sql.slurmtime('10') == 60 * 10  # default is min
    assert slurm2sql.slurmtime('3-10:00') == 3600*24*3 + 10*3600
    assert slurm2sql.slurmtime('3-13:10:00') == 3600*24*3 + 13*3600 + 600
    assert slurm2sql.slurmtime('3-13:10') == 3600*24*3 + 13*3600 + 600
    assert slurm2sql.slurmtime('3-13') == 3600*24*3 + 13*3600

def test_history_last_timestamp(db, slurm_version):
    """Test update_last_timestamp and get_last_timestamp functions"""
    import io
    # initialize db with null input - this just forces table creation.
    slurm2sql.slurm2sql(db, raw_sacct=io.StringIO())
    # Set last update and get it again immediately
    slurm2sql.update_last_timestamp(db, 13)
    assert slurm2sql.get_last_timestamp(db) == 13

def test_history_resume_basic(db, data1):
    """Test --history-resume"""
    # Run it once.  Is the update_time approximately now?
    slurm2sql.main(['dummy', '--history-days=1'], csv_input=data1, db=db)
    update_time = slurm2sql.get_last_timestamp(db)
    assert abs(update_time - time.time()) < 5
    # Wait 1s, is update time different?
    time.sleep(1.1)
    slurm2sql.main(['dummy', '--history-resume'], csv_input=data1, db=db)
    assert update_time != slurm2sql.get_last_timestamp(db)

def test_history_resume_timestamp(db, data1, caplog):
    """Test --history-resume's exact timestamp"""
    # Run once to get an update_time
    slurm2sql.main(['dummy', '--history-days=1'], csv_input=data1, db=db)
    update_time = slurm2sql.get_last_timestamp(db)
    caplog.clear()
    # Run again and make sure that we filter based on that update_time
    slurm2sql.main(['dummy', '--history-resume'], csv_input=data1, db=db)
    assert slurm2sql.slurm_timestamp(update_time) in caplog.text

@pytest.mark.parametrize(
    "string,version",
    [("slurm 20.11.1", (20, 11, 1)),
     ("slurm 19.5.0", (19, 5, 0)),
     ("slurm 19.05.7-Bull.1.0", (19, 5, 7)),
    ])
def test_slurm_version(string, version):
    """Test slurm version detection"""
    v = slurm2sql.slurm_version(cmd=['echo', string])
    assert v == version


# Test slurm 20.11 version
#@pytest.mark.parametrize('slurm_version_number', [(20, 12, 5)])
def test_slurm2011_gres(db, data2):
    """Test 20.11 compatibility, using ReqTRES instead of ReqGRES.

    This asserts that the ReqGRES column is *not* in the database with Slurm > 20.11
    """
    test_slurm2sql_basic(db, data2)
    with pytest.raises(sqlite3.OperationalError, match='no such column:'):
        db.execute('SELECT ReqGRES FROM slurm;')



#
# JobIDs
#
jobid_test_data = [
    # raw text         JobIDonly   ArrayTaskID      JobStep              JobID
    ['7099567_5035',     7099567,         5035,        None,     '7099567_5035',  ],
    ['7102250',          7102250,         None,        None,          '7102250',  ],
    ['1000.2',              1000,         None,         '2',           '1000.2',  ],
    ['1000_2',              1000,            2,        None,           '1000_2',  ],
    ['1000_2.3',            1000,            2,         '3',        '1000_2.3',   ],
    ['1000+2',              1000,         None,        None,           '1000+2',  ],
    ['1000+2.3',            1000,         None,         '3',        '1000+2.3',   ],
    ['1000_2+3',            1000,            2,        None,         '1000_2+3',  ],
    ['1000_2+3.1',          1000,            2,         '1',      '1000_2+3.1',   ],
#    [, , , , ]
    ]
jobidraw_test_data = [
    # raw text        jobIDrawplain
    ['7099567',             7099567, ],
    ['7102250.1',           7102250, ],
    ]
@pytest.mark.parametrize("text, jobidonly, arraytaskid, jobstep, jobid", jobid_test_data)
def test_jobids(text, jobidonly, arraytaskid, jobstep, jobid):
    assert slurm2sql.slurmJobIDonly.calc({'JobID': text}) == jobidonly
    assert slurm2sql.slurmArrayTaskID.calc({'JobID': text}) == arraytaskid
    assert slurm2sql.slurmJobStep.calc({'JobID': text}) == jobstep
    assert slurm2sql.slurmJobID.calc({'JobID': text}) == jobid

@pytest.mark.parametrize("text, jobidrawonly", jobidraw_test_data)
def test_jobidraws(text, jobidrawonly):
    assert slurm2sql.slurmJobIDrawonly.calc({'JobIDRaw': text}) == jobidrawonly



#
# Test data generation
#
def make_test_data():
    """Create current testdata from the slurm DB"""
    slurm_cols = tuple(c for c in slurm2sql.COLUMNS.keys() if not c.startswith('_'))
    lines = slurm2sql.sacct(slurm_cols, ['-S', '2019-08-01', '-E', '2019-08-31'])
    f = open('tests/test-data1.txt', 'w')
    for line in lines:
        line = line.replace(getpass.getuser(), 'user1')
        f.write(line)
    f.close()



if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'maketestdata':
        make_test_data()
