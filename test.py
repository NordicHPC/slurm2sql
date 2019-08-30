
# pylint: disable=redefined-outer-name
import getpass
import os
import sqlite3
import sys
import tempfile
import time

import pytest

import slurm2sql

has_sacct = os.system('sacct --version') == 0


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

@pytest.fixture(scope='function')
def data1():
    """Test data set 1"""
    lines = open('tests/test-data1.txt')
    yield lines


#
# Tests
#
def test_slurm2sql_basic(db, data1):
    slurm2sql.slurm2sql(db, sacct_filter=data1)
    r = db.execute("SELECT JobName, StartTS "
                   "FROM slurm WHERE JobID='43974388';").fetchone()
    assert r[0] == 'spawner-jupyterhub'
    assert r[1] == 1564601354

def test_main(db, data1):
    slurm2sql.main(['dummy'], lines=data1, db=db)
    r = db.execute("SELECT JobName, StartTS "
                   "FROM slurm WHERE JobID='43974388';").fetchone()
    assert r[0] == 'spawner-jupyterhub'
    assert r[1] == 1564601354
    assert db.execute("SELECT count(*) from slurm;").fetchone()[0] == 5

def test_jobs_only(db, data1):
    """--jobs-only gives two rows"""
    slurm2sql.main(['dummy', '--jobs-only'], lines=data1, db=db)
    assert db.execute("SELECT count(*) from slurm;").fetchone()[0] == 2


#
# Test command line
#
@pytest.mark.skipif(not has_sacct, reason="Can only be tested with sacct")
def test_cmdline(dbfile):
    os.system('python3 slurm2sql.py %s -- -S 2019-08-10'%dbfile)
    os.system('python3 slurm2sql.py %s -- -S 2019-08-01 -E 2019-08-02'%dbfile)
    sqlite3.connect(dbfile).execute('SELECT JobName from slurm;')

@pytest.mark.skipif(not has_sacct, reason="Can only be tested with sacct")
def test_cmdline_history_days(dbfile):
    os.system('python3 slurm2sql.py --history-days=10 %s --'%dbfile)
    sqlite3.connect(dbfile).execute('SELECT JobName from slurm;')

@pytest.mark.skipif(not has_sacct, reason="Can only be tested with sacct")
def test_cmdline_history_start(dbfile):
    os.system('python3 slurm2sql.py --history-start=2019-08-25 %s --'%dbfile)
    sqlite3.connect(dbfile).execute('SELECT JobName from slurm;')

@pytest.mark.skipif(not has_sacct, reason="Can only be tested with sacct")
def test_cmdline_history(dbfile):
    print('x')
    os.system('python3 slurm2sql.py --history=2-10 %s --'%dbfile)
    sqlite3.connect(dbfile).execute('SELECT JobName from slurm;')


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
