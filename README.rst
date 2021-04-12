Convert Slurm accounting database to sqlite3 file
=================================================

This contains one utility, ``slurm2sql``, which uses the `Slurm
<https://slurm.schedmd.com/overview>`__ workload manager's ``sacct``,
to export all statistics from jobs and load them to a well-formed
sqlite3 file.  This file can then be queried for analytics much more
easily than the raw database or your own exports.



Installation
------------

There is only a single file with no dependencies.  Python greater than
2.7 is required (dependencies are purposely kept minimal).

The ``slurm2sql`` library + command line frontend can be installed via
the Python Package Index: ``pip install slurm2sql``.



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
order to efficiently keep a running database.  The ``-u`` option means
"don't delete existing database" (jobs with the same JobID get
updated, not duplicated)::

  slurm2sql.py --history-days=N -u sincejuly.sqlite3 -- -a

The ``--history-start=YYYY-MM-DD`` option can do a similar thing
starting from a certain day, and ``--history=DD-HH:MM:SS`` starts
collecting from a given interval of time ago (the time format is as in
Slurm).

To resume from where you left off, first run with one of the history
options.  Then, you can do ``--history-resume`` (no ``-u`` needed) and
it will continue fetching day-by-day from the time you last fetched::

  slurm2sql.py --history-days=N -u sincejuly.sqlite3 -- -a
  slurm2sql.py --history-resume sincejuly.sqlite3 -- -a



It can also be used from Python as what is essentially a glorified
parser::

  db = sqlite3.connect(':memory:')
  slurm2sql.slurm2sql(db, ['-S', '2019-08-26'])

  # For example, you can then convert to a dataframe:
  import pandas as pd
  df = pd.read_sql('SELECT * FROM slurm', db)


Database format
---------------

There is one table with name ``slurm``.  There is one view
``allocations`` which has only the jobs (not job steps) (``where
JobStep is null``).

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

Columns
~~~~~~~

All column values are converted to standard units: *bytes* (not MB,
KB, etc), *seconds*, *fraction 0.0-1.0* for things like
percentages.

Below are some notable columns which do not exist in sacct (for the
rest, check out the `sacct manual page <https://slurm.schedmd.com/sacct.html#lbAF>`_).  It's good
to verify that any of our custom columns make sense before trusting
them.  For other columns, check ``man sacct``.

* ``Time``: approximation of last active time of a job.  The first of
  these that exists: ``End``, ``Start``, ``Submitted``.  This is
  intended to be used when you need to classify a job by when it ran,
  but you don't care to be that specific.  (Only the Time column is
  indexed by default, not the other times)

* ``Submit``, ``Start``, ``End``: like the sacct equivalents,
  but unixtime.  Assume that the sacct timestamps are in localtime of
  the machine doing the conversion.  (``slurm2sql.unixtime`` converts
  slurm-format timestamp to unixtime)

* Job IDs.  Slurm Job ID is by default of format
  ``JobID.JobStep`` or ``ArrayJobID_ArrayTaskID.JobStep``.
  Furthermore, each array job has a "Raw JobID" (different for each
  job, and is an actual JobID) in addition to the "ArrayJobID" which
  is the same for all jobs in an array.  We split all of these
  different IDs into the following fields:

  * ``JobID``: Only the integer Job ID, without the trailing array
    tasks or job IDs.  For array jobs, this is the "Raw JobID" as
    described above, use ``ArrayJobID`` to filter jobs that are the
    same.  Integer

  * ``ArrayJobID``: The common array ID for all jobs in an array -
    only.  For non-array jobs, same as JobID.  Integer or null.

  * ``ArrayTaskID``: As used above.  Integer on null.

  * ``JobStep``: Job step - only.  If you SQL filter for ``StepID is
    null`` you get only the main allocations.  String.

  * ``JobIDSlurm``: The raw output from sacct JobID field, including
    ``.`` and ``_``.  String.

* ``ReqMem``: The raw slurm value in a format like "5Gn".  Instead of
  parsing this, you probably want to use one of the other values below.

* ``ReqMemNode``, ``ReqMemCPU``: Requested memory per node or CPU,
  either taken from ReqMem (if it matches) or computed (you might want
  to check our logic if you rely on this).  In Slurm, you
  can request memory either per-node or per-core, and this calculates
  the other one for you.

* ``ReqMemType``: ``c`` if the user requested mem-per-core originally,
  ``n`` if mem-per-node.  Extracted from ``ReqMem``.

* ``ReqMemRaw``: The numeric value of the ``ReqMem``, whether it is
  ``c`` or ``n``.

* ``ReqGPU``: Number of GPUs requested.  Extracted from ``ReqGRES``.

* GPU information.  At Aalto we store GPU usage information in the
  ``Comment`` field in JSON of the form ``{"gpu_util": NN.NN,
  "gpu_max_mem": NN, "ngpu": N}``.  This extracts information from that.

  * ``GPUMem``: Max amount of memory used from any GPU.  Note: all GPU
    stats require a separate Aalto-developed script.

  * ``GPUEff``: Percent usage of the GPUs (0.0-1.0).

  * ``NGPU``: Number of GPUs.  Should be the same as ``ReqGPU``, but
    who knows.

* ``MemEff``: Memory efficiency (0.0-1.0).  Like in ``seff``.  We
  compute it ourselves, so it could be wrong.  Test before trusting!
  There can still be corner cases, job steps may be off, etc.  This
  also relies on memory reporting being correct, which may not be the
  case...

* ``CPUEff``: CPU efficiency (0.0-1.0).  All the same caveats as above
  apply: test before trusting.

Quick reference of the other most important columns from the
accounting database:

* ``Elapsed``: Wall clock time

* ``CPUTime``: Reserved CPU time (Elapsed * number of CPUs).  CPUEff ≈
  TotalCPU/CPUTime = TotalCPU/(NCPUs x Elapsed)

* ``TotalCPU``: SystemCPU + TotalCPU, seconds of productive work.



Changelog
---------

0.9.1

* Slurm >= 20.11 deprecates the ``AllocGRES`` and ``ReqGRES`` columns
  (using ``Alloc/ReqTRES`` instead).

  * From this slurm2sql version, a ReqTRES column will be requested
    and databases will need to be re-created (or manually added to the
    databases).
  * If run on Slurm > 20.11, it will not request ReqGRES and only use
    ReqTRES.



Development and maintenance
---------------------------

This could be considered beta right now, but it works and is in use by
people.  There are many different variations of Slurm, if it doesn't
work for you, send an issue or pull request to help us make it more
general - development is only done in response to feedback.

Release process::

  python setup.py sdist bdist_wheel
  twine upload [--repository-url https://test.pypi.org/legacy/] dist/*0.9.0*


Originally developed at Aalto University, Finland.
