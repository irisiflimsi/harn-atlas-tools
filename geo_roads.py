#!/usr/bin/python
"""
Connects all roads with either endpoints or the next closest road
if within reasonable distance.
"""
import sys
import argparse
import psycopg2

EPSG = 0.005 # gap to bridge

def make_adj_lines(args, cursor, pt_lines, index):
    """Make adjacent line and connect."""
    for pt_line in pt_lines:
        if args.verbose:
            print(f"- start/end {pt_line[1]} on {pt_line[0]}")
        cursor.execute(f"""
          UPDATE {args.table}_lines
          SET wkb_geometry = ST_Snap(wkb_geometry, '{pt_line[2]}'::geometry, {EPSG*1.01})
          WHERE id = {pt_line[0]}
        """)
        cursor.execute(f"""
          UPDATE {args.table}_lines
          SET wkb_geometry = ST_SetPoint(wkb_geometry, {index}, '{pt_line[2]}'::geometry)
          WHERE id = {pt_line[1]}
        """)

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Create roads from postgis database.')
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
    cursor.execute(f"""
      CREATE TEMP SEQUENCE IF NOT EXISTS serial START 200000;
      ALTER TABLE {args.table}_lines ALTER id SET NOT NULL;
      DELETE FROM {args.table}_lines WHERE type = 'ROUTE';
      SELECT count(*) FROM {args.table}_lines WHERE type LIKE '%ROADS%'
    """)
    print(f"Identifying lines: {cursor.fetchall()[0][0]}")

    # '%00%','%Abbey%','%BRIDGE%','%Battle Site%','%Castle%',
    # '%Chapter House%','%City%','%Ferry%','%Ford%','%Fort%','%Gargun%',
    # '%Keep%','%Manor%','%Mine%','%PEAK%','%Quarry%','%ROAD%' <- Tollhouse
    # '%Rapids%','%Ruin%','%SEA%','%SWAMP%','%Salt%','%Special %',
    # '%special %','%Swamp%','%TOWNS%','%Tribal%','%Waterfall%'
    sql_locs = "type LIKE '%Abbey%' OR \
          type LIKE '%BRIDGE%' OR \
          type LIKE '%Chapter House%' OR \
          type LIKE '%City' OR \
          type LIKE '%Ferry%' OR \
          type LIKE '%Ford%' OR \
          type LIKE '%Fort%' OR \
          type LIKE '%Gargun%' OR \
          type LIKE '%Keep%' OR \
          type LIKE '%Manor%' OR \
          type LIKE '%Mine%' OR \
          type LIKE '%Quarry%' OR \
          type LIKE '%ROAD%' OR \
          type LIKE '%Salt%' OR \
          type LIKE '%Special%' OR \
          type LIKE '%special%' OR \
          type LIKE '%TOWNS%' OR \
          type LIKE '%Tribal%' OR \
          type LIKE '%Castle%'"

    # Get all locations
    cursor.execute(f"""
      SELECT ST_Union(wkb_geometry) FROM {args.table}_pts
      WHERE {sql_locs}
    """)
    pts = cursor.fetchall()[0][0]

    # Shift all roads onto locations
    cursor.execute(f"""
      SELECT t1.id, array_agg(t2.id), t1.wkb_geometry
      FROM {args.table}_pts AS t1 INNER JOIN LATERAL (
        SELECT id, wkb_geometry FROM {args.table}_lines
        WHERE type LIKE '%ROADS%' AND
          ST_Distance(wkb_geometry, t1.wkb_geometry) < {EPSG} AND
          ST_Distance(wkb_geometry, t1.wkb_geometry) <> 0
      )
      AS t2 (id, geo) ON TRUE
      WHERE {sql_locs}
      GROUP BY t1.id
    """)
    pt_lines = cursor.fetchall()
    print(f"Shift {len(pt_lines)} roads onto locations")
    for pt_line in pt_lines:
        if args.verbose:
            print(f"- shift onto {pt_line[0]}")
        for pt_i in pt_line[1]:
            cursor.execute(f"""
              UPDATE {args.table}_lines
              SET wkb_geometry = ST_Snap(wkb_geometry, '{pt_line[2]}'::geometry, {EPSG*1.01})
              WHERE id = {pt_i}""")

    # Shift all road starts/ends
    cursor.execute(f"""
      SELECT t1.id, t2.id, ST_ClosestPoint(t1.wkb_geometry, ST_StartPoint(t2.geo))
      FROM {args.table}_lines AS t1 INNER JOIN LATERAL (
        SELECT t3.id, t3.wkb_geometry FROM {args.table}_lines AS t3
        WHERE t3.id <> t1.id AND t3.type LIKE '%ROADS%' AND
          ST_Distance(ST_StartPoint(t3.wkb_geometry), t1.wkb_geometry) < {EPSG} AND
          ST_Distance(ST_StartPoint(t3.wkb_geometry), '{pts}'::geometry) > {EPSG/2}
      )
      AS t2 (id, geo) ON TRUE
      WHERE t1.type LIKE '%ROADS%'
    """)
    pt_lines = cursor.fetchall()
    print(f"Shift {len(pt_lines)} road-starts onto roads")
    # Make adjacent lines include new start points and ending lines end in new start points
    make_adj_lines(args, cursor, pt_lines, 0)

    cursor.execute(f"""
      SELECT t1.id, t2.id, ST_ClosestPoint(t1.wkb_geometry, ST_EndPoint(t2.geo))
      FROM {args.table}_lines AS t1 INNER JOIN LATERAL (
        SELECT t3.id, t3.wkb_geometry FROM {args.table}_lines AS t3
        WHERE t3.id <> t1.id AND t3.type LIKE '%ROADS%' AND
          ST_Distance(ST_EndPoint(t3.wkb_geometry), t1.wkb_geometry) < {EPSG} AND
          ST_Distance(ST_EndPoint(t3.wkb_geometry), '{pts}'::geometry) > {EPSG/2}
      )
      AS t2 (id, geo) ON TRUE
      WHERE t1.type LIKE '%ROADS%'""")
    pt_lines = cursor.fetchall()
    print(f"Shift {len(pt_lines)} road-end onto roads")
    # Make adjacent line include new end point and ending line end in new end point
    make_adj_lines(args, cursor, pt_lines, -1)

    print(f"Remove some artifacts")
    cursor.execute(f"""
      SELECT id, ST_NPoints(wkb_geometry) FROM {args.table}_lines
      WHERE type LIKE '%ROADS%' AND
        ST_Distance(ST_StartPoint(wkb_geometry), '{pts}'::geometry) < {EPSG} AND
        ST_Distance(ST_StartPoint(wkb_geometry), '{pts}'::geometry) <> 0
    """)
    pt_lines = cursor.fetchall()
    for pt_line in pt_lines:
        if pt_line[1] > 2:
            cursor.execute(f"""
              UPDATE {args.table}_lines
              SET wkb_geometry = ST_RemovePoint(wkb_geometry, 0)
              WHERE id = {pt_line[0]}
            """)
        else:
            cursor.execute(f"""
              DELETE FROM {args.table}_lines
              WHERE id = {pt_line[0]}
            """)
    cursor.execute(f"""
      SELECT id, ST_NPoints(wkb_geometry) FROM {args.table}_lines
      WHERE type LIKE '%ROADS%' AND
        ST_Distance(ST_EndPoint(wkb_geometry), '{pts}'::geometry) < {EPSG} AND
        ST_Distance(ST_EndPoint(wkb_geometry), '{pts}'::geometry) <> 0
    """)
    pt_lines = cursor.fetchall()
    for pt_line in pt_lines:
        if pt_line[1] > 2:
            cursor.execute(f"""
              UPDATE {args.table}_lines
              SET wkb_geometry = ST_RemovePoint(wkb_geometry, {pt_line[1] - 1})
              WHERE id = {pt_line[0]}
            """)
        else:
            cursor.execute(f"""
              DELETE FROM {args.table}_lines
              WHERE id = {pt_line[0]}
            """)

    print(f"Make all trails")
    cursor.execute(f"""
      INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'Trail', t1.geo FROM (
        SELECT (ST_Dump(ST_LineMerge(ST_Union(wkb_geometry)))).geom FROM {args.table}_lines
        WHERE type LIKE '%ROADS%' AND style LIKE '%dasharray: 1 1%'
      )
      AS t1 (geo)
    """)
    print(f"Make all unpaved roads")
    cursor.execute(f"""
      INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'Unpaved', t1.geo FROM (
        SELECT (ST_Dump(ST_LineMerge(ST_Union(wkb_geometry)))).geom FROM {args.table}_lines
        WHERE type LIKE '%ROADS%' AND style LIKE '%dasharray: 2 1%'
      )
      AS t1 (geo)""")
    print(f"Make all paved roads")
    cursor.execute(f"""
      INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'Paved', t1.geo FROM (
        SELECT (ST_Dump(ST_LineMerge(ST_Union(wkb_geometry)))).geom FROM {args.table}_lines
        WHERE type LIKE '%ROADS%' AND style NOT LIKE '%dasharray:%'
      )
      AS t1 (geo)
    """)

    conn.commit()

if __name__ == '__main__':
    main()
