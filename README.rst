Read a Slurm accounting database to a sqlite3 file
==================================================

This contains a utility, ``slurm2sql``, which uses the `Slurm
<https://slurm.schedmd.com/overview>`__ workload manager's ``sacct``,
to export statistics from jobs and load them to a well-formed SQLite3
file (the database is also made so that it can be quried with DuckDB).
This file can then be queried for analytics much more easily than the
raw database or your own exports.  The main features are:

- Parse ``sacct`` output (this was made before it had JSON output,
  which you might want to look at instead - it's hard to use though).
- Preprocess all the values in to basic units, including values like
  GPU usage that currently have to be extracted from other fields.

Even if SQLite isn't what you need, it provides an easy intermediate
file on the way to convert to whatever format you want.  In
particular, it defines the database so that it can be used with
DuckDB, which is a more efficient tool for analytics.

There are also some command line frontends, ``slurm2sql-sacct`` and
``slurm2sql-seff`` that use this parsing to print out data in better
forms than built-in Slurm commands.  This is especially useful for
``sacct``.  You can design your own tools like this.



Installation
------------

Normal ``pip`` installation, name ``slurm2sql`` for the command line
programs.  This installs the library and command line programs.

::

   pip install slurm2sql

There is only a single file with no depencecies for the core
``slurm2sql`` library (which could also be manually downloaded - HPC,
right?), though the command line programs require ``tabulate``.  It's
made to support very old Python.



Usage
-----


``slurm2sql``
~~~~~~~~~~~~~

Sample usage::

  slurm2sql.py OUTPUT_DB -- [SACCT_FILTER_OPTIONS]


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
it will continue fetching day-by-day from the time you last fetched.
You can also run this every day, to first load old historykeep a database updated::

  slurm2sql.py --history-days=N -u sincejuly.sqlite3 -- -a
  slurm2sql.py --history-resume sincejuly.sqlite3 -- -a


``slurm2sql-sacct``
~~~~~~~~~~~~~~~~~~~

This probably isn't the most useful part.  Look at command line options.

.. code-block:: console

   $ slurm2sql-sacct SACCT_FILTER


``slurm2sql-seff``
~~~~~~~~~~~~~~~~~~

This is more useful: it prints ``seff`` like output in a tabular
format.  MemReqGiB is per-node, to compare withMaxRSSGiB.

.. code-block:: console

   $ slurm2sql-sacct SACCT_FILTER

.. code-block:: console

   $ slurm2sql-seff -S now-3day
     JobID User    hours NCPUS CPUeff MemReqGiB MaxRSSGiB MemEff NGpus GPUeff read_MiBps write_MiBps
   ------- ------- ----- ----- ------ --------- --------- ------ ----- ------ ---------- -----------
   1860854 darstr1  0.28     1    87%     50         9.76    20%                  213.88       14.51
   1877467 darstr1  0        0     0%      0                  0%
   1884493 darstr1  0        1     0%      0.49      0        0%
   1884494 darstr1  0        1     0%      0.49      0        0%


From Python
~~~~~~~~~~~

It can also be used from Python as what is essentially a glorified
parser.

.. code-block:: python

  db = sqlite3.connect(':memory:')
  slurm2sql.slurm2sql(db, ['-S', '2019-08-26'])

  # For example, you can then convert to a dataframe:
  import pandas as pd
  df = pd.read_sql('SELECT * FROM slurm', db)


From DuckDB
~~~~~~~~~~~

DuckDB is a lot like SQLite, but column-oriented and optimized for
fast processing of data.  The main downsides are slow inserts and
columns must have consistent data types, but that's the tradeoff we
need.  Slurm2sql's SQLite database is created with type definitions,
so that you can easily open it with DuckDB even without conversion:

.. code-block:: console

   $ duckdb dump.sqlite3

Or for even more speed, make a temporary in-memory copy (or this could
also be made into a file):

.. code-block:: sql

   -- command line:  $ duckdb database.db
   ATTACH ':memory:' AS tmp;
   CREATE TABLE tmp.slurm AS (SELECT * FROM slurm);
   USE tmp;      -- optional but makes tmp the default

Converting to DuckDB:

.. code-block:: console

    $ duckdb new.duckdb "CREATE TABLE slurm AS (SELECT * FROM sqlite_scan('original.sqlite3', 'slurm'))"

Using via DuckDB from Python (with the raw sqlite database):

.. code-block:: python

    conn = duckdb.connect("database.sqlite3")
    conn.execute("select avg(cputime) from slurm").df()



Database format
---------------

Tables and views:

* Table ``slurm``: the main table with all of the data.  There is one
  row for each item returned by ``sacct``.
* View ``allocations``: has only the jobs (not job steps) (``where
  JobStep is null``).
* View ``eff``: Does a lot of processing of ``slurm`` to produce some
  ``CPUEff``, ``MemEff``, and ``GPUeff`` values (0.0-1.0 usage
  fractions), in addition to a bit more.

In general, there is one column for each item returned by ``sacct``,
but some of them are converted into a more useful form.  Some columns
are added by re-processing other columns.  See ``COLUMNS`` in
``slurm2sql.py`` for details.  Extra columns can easily be added.

Developer note: There are two types of converter functions to make the
columns: easy ones, which map one slurm column directly to a database
column via a function, and line functions, which take the whole row
and can do arbitrary remixing of the data (to compute things like CpuEff.

Columns
~~~~~~~

All column values are converted to standard units: *bytes* (not MB,
KB, etc), *seconds*, *fraction 0.0-1.0* for things like
percentages, and *unixtime*.

Columns which are the same in raw ``sacct`` output aren't documented
specifically here (but note the default units above).

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

* ``QueueTime`` is Start-Submit in seconds.  Start/End do not include
  timezones, so expect inaccuracies around summer time changes.

* Job IDs.  Slurm Job ID is by default of format
  ``JobID.JobStep`` or ``ArrayJobID_ArrayTaskID.JobStep``.
  Furthermore, each array job has a "Raw JobID" (different for each
  job, and is an actual JobID) in addition to the "ArrayJobID" which
  is the same for all jobs in an array.  We split all of these
  different IDs into the following fields:

  * ``JobID``: The full raw value that Slurm gives.  The same for each
    job in an array.

    Only the integer Job ID, without the trailing array
    tasks or job IDs.  For array jobs, this is the "Raw JobID" as
    described above, use ``ArrayJobID`` to filter jobs that are the
    same.  Integer

  * ``JobIDnostep``: The part of JobID without anything after the ``.``
    (no steps)

  * ``JobIDonly``: The integer part of the JobID.

  * ``JobIDRawonly``: The integer part of the Raw JobID (so this is
    different for each job in an aray).

  * ``ArrayTaskID``: As used above.  Integer on null.

  * ``JobStep``: Job step - only.  If you SQL filter for ``StepID is
    null`` you get only the main allocations.  String.

  * Note: HetJob offsets are not currently handled and silently
    stripped out and give invalid data.  File an issue and this will
    be added.

* **Memory related**

  * ``AllocMem``: The ``mem=`` value from ``AllocTRES`` field.  You
    probably want to use this.

  * ``TotalMem``: The ``mem=`` value from ``TRESUsageInTot`` field.
    You probably want to use this.

  * ``ReqMem``: The raw slurm value from the ReqMem column.

  * ``ReqMemNode``, ``ReqMemCPU``: Requested memory per node or CPU,
    ``ReqMem`` / ``NNodes``.

  * ``MemEff``: Computed ``TotalMem / AllocMem``.

* **GPU information.**  These use values from the ``TRESUsageInAve``
  fields in modern Slurm

  * ``ReqGPU``: Number of GPUs requested.  Extracted from ``ReqTRES``.

  * ``GpuMem``: ``gres/gpumem`` from ``TRESUsageInAve``

  * ``GpuUtil``: ``gres/gpuutil`` (fraction 0.0-1.0).

  * ``NGpus``: Number of GPUs from ``gres/gpu`` in ``AllocTRES``.
    Should be the same as ``ReqGPU``, but who knows.

  * ``GpuUtilTot``, ``GpuMemTot``: like above but using the
    ``TRESUsageInTot`` sacct field.

  * ``GpuEff``: ``gres/gpuutil`` (from ``TRESUsageInTot``) / (100 *
    ``gres/gpu`` (from ``AllocTRES``).

* ``CPUEff``: CPU efficiency (0.0-1.0).  All the same caveats as above
  apply: test before trusting.

* And more, see the code for now.

Quick reference of the other most important columns from the
accounting database that are hardest to remember:

* ``Elapsed``: Wall clock time

* ``CPUTime``: Reserved CPU time (Elapsed * number of CPUs).  CPUEff ≈
  TotalCPU/CPUTime = TotalCPU/(NCPUs x Elapsed)

* ``TotalCPU``: SystemCPU + TotalCPU, seconds of productive work.

The ``eff`` table adds the following:

* ``CPUEff``: Highest CPUEff for any job step

* ``MemEff``: Highest MemEff for any job step

* ``GpuEff``: Highest GpuEff for any job step



Changelog
---------

Next

* This is the biggest column clean-up in a while.
* Add slurm2sql-{seff,sacct} commands.
* JobID columns adjusted: ``JobID`` is the raw thing that slurm gives,
  ``*only`` integer IDs without any trailing things,
  ``JobIDrawonly`` is the RawJobID without any trailing things.
* ReqMem has been updated: it no longer parses ``n`` and ``c``
  suffixes for mem-per-node/cpu, and that respective column has been
  removed.
* MemEff has been removed from the ``slurm`` table, since it is always
  empty.  The ``eff`` view has been added instead.

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
people.  If this is important for you, comment about your use case
in the Github issue tracker.  Watch the repo if you want to give
comments on future data schema updates and backwards compatibility
when Slurm changes.

There are many different variations of Slurm, if it doesn't
work for you, send an issue or pull request to help us make it more
general - development is only done in response to feedback.

Development principles:

- All values are the most basic (metric) units: bytes, seconds,
  seconds-since-epoch, etc.
- Try to use existing Slurm column names as much as possible (even if
  they are hard to remember).
- Try to support as many Slurm versions as possible, but if something
  becomes hard to support, don't worry too much about breaking
  compatibly. SchedMD support slurm for 18 months after release. Try
  to support at least those versions.  (Until someone asks for it,
  don't assume we can import data from very old Slurm versions)
- Don't try to maintain database compatibility. It's expected that for
  all schema changes, you have to delete and re-import. But try to
  avoid this if not needed.

Release process::

  python setup.py sdist bdist_wheel
  twine upload [--repository-url https://test.pypi.org/legacy/] dist/*0.9.0*

Originally developed at Aalto University, Finland.
