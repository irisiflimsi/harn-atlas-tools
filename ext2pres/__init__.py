#!/usr/bin/python
"""
Constructor artifact. See __main__.py for package docs.
"""
import psycopg2

DBC = "user:password@dbname:localhost:25432"

CONNECT = psycopg2.connect(
    user=f"{DBC.split('@')[0].split(':')[0]}",
    password=f"{DBC.split('@')[0].split(':')[1]}",
    database=f"{DBC.split('@')[1].split(':')[0]}",
    host=f"{DBC.split('@')[1].split(':')[1]}",
    port=f"{DBC.split('@')[1].split(':')[2]}"
)

CURSOR = CONNECT.cursor()

def sql(*args):
    """Shorthand for execute and fetchall."""
    if len(args) > 0:
        CURSOR.execute(args[0])
        return None
    return CURSOR.fetchall()
