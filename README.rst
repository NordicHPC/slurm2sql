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



Development and maintenance
---------------------------

This could be considered functional alpha right now.
