Convert Slurm accounting database to sqlite3 file
=================================================

This contains one utility, ``slurm2sql``, which uses the `Slurm
<https://slurm.schedmd.com/overview>`__ workload manager's ``sacct``,
to export all statistics from jobs and load them to a well-formed
sqlite3 file.  This file can then be queried for analytics much more
easily than the raw database or your own exports.



Installation
-----------

There is only a single file with no dependencies.  Python3 is required.



Usage
-----

Sample usage::

  slurm2sql.py [output_db] -- [sacct selection options]


For example, to get all data from July and August (``-S``) for all
users (``-a``)::

  slurm2sql.py sincejuly.sqlite3 -- -S 2019-07-1 -a


To get the data from the last *N* days.  This will, day by day, get
each of these history and cumulatively update the database.  This
updates a database by default, so that it can be used every day in
order to efficiently keep a running database.::

  slurm2sql.py --days-history=N sincejuly.sqlite3 -- -a


It can also be used from Python::

  db = sqlite3.connect(':memory:')
  slurm2sql.slurm2sql(db, ['-S', '2019-08-26'])

  # For example, you can then convert to a dataframe:
  import pandas as pd
  df = pd.read_sql('SELECT * FROM slurm', db)


Database format
---------------

There is one table with name ``slurm``.

There is one row for each item returned by ``sacct``.

In general, there is one column for each item returned by ``sacct``,
but some of them are converted into a more useful form.  Some columns
are added by re-processing other columns.  In general, just use the
source.  See ``COLUMNS`` in ``slurm2sql.py`` for details.  Extra
columns can easily be added.

There are two types of converter functions: easy ones, which map one
slurm column directly to a database column via a function, and line
functions, which take the whole row and can do arbitrary remixing of
the data.



Development and maintenance
---------------------------

This could be considered functional alpha right now.
