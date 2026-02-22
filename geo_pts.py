#!/usr/bin/python
"""
Create peak names and height.
"""
import argparse
import sys
import psycopg2

def create_peaks(args, cursor):
    """Associate names to peaks"""
    print("Label peaks")
    cursor.execute(f"""
      SELECT t1id, substring(name for 1) || lower(substring(rtrim(name,'0123456789') FROM 2)),
        ltrim(name,'A''BCDEFGHIJKLMNOPQRSTUVWXYZ')
      FROM (
        SELECT t1.id AS t1id, substring(name from 7) AS t2name, dist FROM xyz_pts AS t1, LATERAL (
          SELECT t2.name AS name, ST_Distance(t1.wkb_geometry, t2.wkb_geometry) AS dist
          FROM xyz_pts AS t2 WHERE t2.name LIKE '%PEAKN%'
          ORDER BY ST_Distance(t1.wkb_geometry, t2.wkb_geometry) LIMIT 1
        )
        WHERE t1.type = 'PEAK' AND dist < 0.03 ORDER BY dist DESC
      )
    """)
    for row in cursor.fetchall():
        row1 = row[1].replace("'", "''")
        cursor.execute(f"""
          UPDATE {args.table}_pts
          SET name = '{row1}', svgid = {row[2]}
          WHERE id = {row[0]}
        """)

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Create peak information from postgis database')
    parser.add_argument(
        '-d', '--database', dest='db', required=True,
        help='db to connect to user:password@dbname:host:port')
    parser.add_argument(
        '-t', '--table', dest='table', required=True,
        help='table prefix; _pts and _lines will be added')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='verbose', required=False)
    args = parser.parse_args()

    conn = psycopg2.connect(
        user=f"{args.db.split('@')[0].split(':')[0]}",
        password=f"{args.db.split('@')[0].split(':')[1]}",
        database=f"{args.db.split('@')[1].split(':')[0]}",
        host=f"{args.db.split('@')[1].split(':')[1]}",
        port=f"{args.db.split('@')[1].split(':')[2]}")
    cursor = conn.cursor()

    # Initialize
    create_peaks(args, cursor)
    conn.commit()

if __name__ == '__main__':
    main()
