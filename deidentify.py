"""Deidentify certain columns by replacing each unique value with a slurm.
"""

import sqlite3
import sys

COLUMNS = ["JobName", "User", "Group", "SubmitLine", "Comment"]

conn = sqlite3.connect(sys.argv[1])

def deidentify_column(conn, colname):
    """Change identifier to something.

    This version was not efficient enough for production.
    """
    print(f"Beginning {colname}")
    with conn:
        cur = conn.cursor()
        cur.execute(f'SELECT DISTINCT "{colname}" FROM slurm WHERE "{colname}" IS NOT NULL')
        vals = [r[0] for r in cur.fetchall()]
        mapping = {val: f"{colname}_{i}" for i, val in enumerate(vals)}
        print (f"  mapping size: {len(mapping)}")
        for val, i in mapping.items():
            print(f"    executing {val} -> {i}")
            cur.execute(f'UPDATE slurm SET "{colname}" = ? WHERE "{colname}" = ?', (i, val))
        return len(mapping)

def deidentify_column2(conn, colname):
    """Change identifiers of one column to something.
    """
    print(f"Beginning {colname}")
    with conn:
        cur = conn.cursor()
        mapping = {}
        cur.execute(f'SELECT rowid, "{colname}" FROM slurm WHERE "{colname}" IS NOT NULL')
        rows = cur.fetchall()
        print(f"  Total rows {len(rows)}")
        for i, (rowid, val) in enumerate(rows):
            if val in mapping:
                newval = mapping[val]
            else:
                newval = mapping[val] = f"{colname}_{len(mapping)}"
            cur.execute(f'UPDATE slurm SET "{colname}" = ? WHERE rowid = ?', (newval, rowid))
            if i % 100000 == 0:
                print(f"    {colname}: Done value {i} / {len(rows)} ({newval})")
                sys.stdout.flush()
        return len(mapping)


for colname in COLUMNS:
    count = deidentify_column2(conn, colname)
    print(f"colname={colname}, mapped {count} distinct values")
