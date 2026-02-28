#!/usr/bin/python
"""
Create peak names and height.
"""
import argparse
import sys
import psycopg2

def obtain_names(args, cursor):
    """Associate names to peaks"""
    cursor.execute(f"""
      SELECT count(*) FROM {args.table}_pts WHERE name = '-'
    """)
    print(f"Unnamed points...{cursor.fetchall()[0][0]}")

    # Remove pure labels
    print("Remove pure labels")
    cursor.execute(f"""
      DELETE FROM {args.table}_pts
      WHERE type LIKE 'Alpine %' OR
        type LIKE 'Needleleaf %' OR
        type LIKE 'Woodlands %' OR
        type LIKE 'Heath %' OR
        type LIKE 'Snow/Ice%' OR
        type LIKE 'Swamp%' OR
        type LIKE 'Forest %' OR
        type LIKE 'Cropland %' OR
        type LIKE '/WOODLAND/%' OR
        type LIKE '/SWAMPS/%' OR
        type LIKE '/SHOAL/%' OR
        type LIKE '/ROADS/%' OR
        type LIKE '/HEATH/%' OR
        type LIKE '/FOREST/%' OR
        type LIKE '/CROPLAND/%' OR
        type LIKE '/BOUNDARIES%'
    """)

    print("Label peaks")
    cursor.execute(f"""
      SELECT t1id, substring(t2name for 1) || lower(substring(rtrim(t2name,'0123456789') FROM 2)),
        ltrim(t2name,'A''BCDEFGHIJKLMNOPQRSTUVWXYZ')
      FROM (
        SELECT t1.id AS t1id, substring(t2.name from 10) AS t2name, dist FROM {args.table}_pts AS t1,
        LATERAL (
          SELECT t3.name AS name, ST_Distance(t1.wkb_geometry, t3.wkb_geometry) AS dist
          FROM {args.table}_pts AS t3 WHERE t3.name LIKE '%PeakName%'
          ORDER BY ST_Distance(t1.wkb_geometry, t3.wkb_geometry) LIMIT 1
        )
        AS t2
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

    print("Label typed")
    cursor.execute(f"""
      SELECT t1id, substring(t2name for 1) || lower(substring(rtrim(t2name,'0123456789') FROM 2))
      FROM (
        SELECT t1.id AS t1id, substring(t2.name from 9) AS t2name, dist FROM {args.table}_pts AS t1,
        LATERAL (
          SELECT t3.name AS name, ST_Distance(t1.wkb_geometry, t3.wkb_geometry) AS dist
          FROM {args.table}_pts AS t3 WHERE t3.name LIKE '%AnyName%'
          ORDER BY ST_Distance(t1.wkb_geometry, t3.wkb_geometry) LIMIT 1
        )
        AS t2
        WHERE (
            t1.type = 'Abbey' OR
            t1.type LIKE 'BRIDGE%' OR
            t1.type LIKE 'Battle%' OR
            t1.type = 'Castle' OR
            t1.type LIKE 'Chapter%' OR
            t1.type = 'City' OR
            t1.type = 'Ferry' OR
            t1.type = 'Ford' OR
            t1.type LIKE '%Fort%' OR
            t1.type = 'Gargun' OR
            t1.type = 'Keep' OR
            t1.type = 'Mine' OR
            t1.type = 'PEAK' OR
            t1.type = 'Quarry' OR
            t1.type = 'Rapids' OR
            t1.type LIKE 'Ruin%' OR
            t1.type = 'Salt' OR
            t1.type LIKE 'Special%' OR
            t1.type LIKE '%Manor%' OR
            t1.type LIKE 'Tollbooth%' OR
            t1.type LIKE 'Tribal%' OR
            t1.type = 'Tunnel' OR
            t1.type = 'Waterfall'
          )
          AND dist < 0.03 AND (t1.name = '' OR t1.name = '-')
        ORDER BY dist DESC
      )
    """)
    for row in cursor.fetchall():
        row1 = row[1].replace("'", "''")
        cursor.execute(f"""
          UPDATE {args.table}_pts
          SET name = '{row1}'
          WHERE id = {row[0]}
        """)

    # Remaining
    cursor.execute(f"""
      SELECT count(*) FROM {args.table}_pts WHERE name = '-'
    """)
    print(f"Remaining nnamed points...{cursor.fetchall()[0][0]}")

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

    obtain_names(args, cursor)
    conn.commit()

if __name__ == '__main__':
    main()
