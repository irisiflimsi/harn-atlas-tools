#!/usr/bin/python
"""
Evaluates elevation labels when close to a contour and
attaches the elevation to the contour line in output as name. Includes
heuristics for connected lines and rings.
"""
import sys
import argparse
import psycopg2

EPSP = 0.0025
EPSL = 0.007

def verbosity(verb, out):
    """Verbosity."""
    if verb:
        print(f" - {out}")

def shortest_connect(table, cursor, line_id, line_type):
    """
    Returns the id of the closest line, the type, the geometry of it,
    of the original line, and of the connecting line.
    """
    cursor.execute(f"""
      SELECT wkb_geometry FROM {table} WHERE id = {line_id}
    """)
    line_geo = cursor.fetchall()[0][0]
    p_11 = f"(1, ST_StartPoint('{line_geo}'::geometry))"
    p_12 = f"(2, ST_EndPoint('{line_geo}'::geometry))"
    p_21 = f"(1, ST_StartPoint(main.wkb_geometry))"
    p_22 = f"(2, ST_EndPoint(main.wkb_geometry))"
    cursor.execute(f"""
      SELECT add_id, add_type, add_geo, line_geo, connect_geo FROM (
        SELECT main.id, main.type, main.wkb_geometry, '{line_geo}', (
          WITH pts1 (i, p) AS (VALUES {p_11}, {p_12}), 
            pts2 (i, p) AS (VALUES {p_21}, {p_22})
          SELECT ST_MakeLine(pt1.p, pt2.p) FROM pts1 AS pt1 CROSS JOIN pts2 AS pt2
          WHERE (main.id <> {line_id} OR pt1.i <> pt2.i)
          ORDER BY ST_Distance(pt1.p, pt2.p) ASC LIMIT 1
        )
        FROM {table} AS main
      )
      AS connects (add_id, add_type, add_geo, line_geo, connect_geo)
      WHERE ST_Length(connects.connect_geo) < {EPSL} AND
        (connects.add_type LIKE '%CONTOURS%' OR connects.add_type = '{line_type}'
        )
      ORDER BY ST_Length(connects.connect_geo) ASC LIMIT 1
    """)
    ret = cursor.fetchall()
    return ret

def make_valid(table, cursor, merge, line_id):
    """Removes the smallest segments until a single line remains. Update."""
    multi_line = True
    while multi_line:
        sql_array = "'" + "'::geometry, '".join(merge) + "'::geometry"
        cursor.execute(f"""
          SELECT geo FROM (
            SELECT (ST_Dump(ST_LineMerge(ST_Union(ARRAY[{sql_array}])))).geom
          )
          AS lines (geo) ORDER BY ST_Length(geo) DESC
        """)
        merge = cursor.fetchall()
        if len(merge) == 1:
            break
        merge = [m[0] for m in merge[:-1]]

    cursor.execute(f"""
      UPDATE {table}
      SET wkb_geometry = '{merge[0][0]}'::geometry
      WHERE id = {line_id}
    """)

def sort_elevation_pts(table, cursor):
    """Sort all elevation points to their elevation."""
    cursor.execute(f"""
      SELECT substring(type, '[^1-9]([1-9][05]|5)00') AS elev,
        ST_Union(wkb_geometry)
      FROM {table}
      WHERE type LIKE '%00%'
      GROUP BY elev
    """)
    points = cursor.fetchall()
    elevsets = ""
    for pt_i in points:
        elevsets += f"({pt_i[0]}, '{pt_i[1]}'::geometry),"
    return elevsets

def handle_unlabeled_rings(args, cursor):
    """Handle unlabeld rings."""
    print("Unlabeled rings")
    cursor.execute(f"""
      SELECT topring.id, topring.wkb_geometry FROM {args.table}_lines AS topring
      WHERE ST_IsClosed(topring.wkb_geometry) AND (
        topring.type LIKE '%CONTOURS%' OR topring.type LIKE '%00%'
        )
        AND EXISTS (
          SELECT * FROM {args.table}_pts AS peaks
          WHERE type = 'PEAK' AND
            ST_Intersects(ST_MakePolygon(topring.wkb_geometry), peaks.wkb_geometry)
        )
        AND NOT EXISTS (
          SELECT * FROM {args.table}_lines AS covers
          WHERE (covers.type LIKE '%00%' OR covers.type LIKE '%CONTOURS%') AND
            topring.id <> covers.id AND
            CASE WHEN ST_IsClosed(topring.wkb_geometry) THEN
              ST_Covers(ST_MakePolygon(topring.wkb_geometry), covers.wkb_geometry)
            END
        )
    """)
    lines = cursor.fetchall()
    for line in lines:
        label_rings(args.verbose, f"{args.table}_lines", cursor, line)

def label_rings(verbose, table, cursor, line):
    """Label all unlabeled rings and check some incident labeled ones."""
    verbosity(verbose, f"ring {line[0]}")
    cursor.execute(f"""
      SELECT id, type FROM {table}
      WHERE (type LIKE '%CONTOURS%' OR type LIKE '%00%') AND
        CASE WHEN ST_IsClosed(wkb_geometry) THEN
          ST_Covers(ST_MakePolygon(wkb_geometry), '{line[1]}'::geometry)
        END
      ORDER BY ST_Distance(wkb_geometry, '{line[1]}'::geometry) ASC
    """)
    rings = list(enumerate(cursor.fetchall()))
    for idx_r, ring in rings:
        if "00" not in ring[1]:
            continue
        verbosity(verbose, f"- found fixed {ring}")
        for idx_c, check in rings:
            elev = int(ring[1]) + 500*(idx_r - idx_c)
            verbosity(verbose, f"- - fix {check} with {elev}")
            if f"{elev}" != check[1] and "CONTOURS" not in check[1]:
                print(f"- - - errorneous fix {check} with {elev}")
            cursor.execute(f"""
              UPDATE {table}
              SET type = '{elev}'
              WHERE id = {check[0]} AND type LIKE '%CONTOURS%'
            """)

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Create elevation lines from postgis database.')
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
      CREATE TEMP SEQUENCE IF NOT EXISTS serial START 400000;
      ALTER TABLE {args.table}_lines ALTER id SET NOT NULL;
      SELECT count(*) FROM {args.table}_lines WHERE type LIKE '%CONTOURS%'
    """)
    print(f"Identifying lines: {cursor.fetchall()[0][0]}")

    # Remove pathological lines
    print("Remove lines of length 3")
    cursor.execute(f"""
      DELETE FROM {args.table}_lines
      WHERE type LIKE '%CONTOURS%' AND ST_NumPoints(wkb_geometry) < 4
        OR ST_Length(wkb_geometry) < {EPSL}
    """)

    # Special corrections
    print("Remove spurious lines")
    verbosity(args.verbose, f"duplicate at LM5") # delicate
    approx = 'POLYGON((-17.0025 45.7429, ' + \
        '-17.0023 45.7429, -17.0023 45.7426, ' + \
        '-17.0025 45.7426, -17.0025 45.7429))'
    cursor.execute(f"""
      DELETE FROM {args.table}_lines
      WHERE ST_intersects(wkb_geometry, ST_GeomFromText('{approx}'))
    """)
    print("Validate lines")
    cursor.execute(f"""
      SELECT id, wkb_geometry FROM {args.table}_lines WHERE type LIKE '%CONTOURS%'
    """)
    lines = cursor.fetchall()
    for line in lines:
        make_valid(f"{args.table}_lines", cursor, [line[1]], line[0])

    # Match labels and lines
    print("Matching height label to lines")
    elevsets = sort_elevation_pts(f"{args.table}_pts", cursor)
    cursor.execute(f"""
      UPDATE {args.table}_lines
      SET type = t3.b FROM (
        WITH elev (idx, geom) AS (VALUES {elevsets[:-1]})
        SELECT t1.id, t2.idx || '00' FROM {args.table}_lines AS t1 JOIN elev AS t2 ON TRUE
        WHERE ST_Distance(t2.geom, t1.wkb_geometry) < {EPSP}
        ORDER BY ST_Distance(t2.geom, t1.wkb_geometry)
      )
      AS t3 (a, b)
      WHERE id = t3.a AND type LIKE '%CONTOURS%'
    """)

    cursor.execute(f"""
      SELECT count(*) FROM {args.table}_lines WHERE type LIKE '%CONTOURS%'
    """)
    print(f"Remaining lines: {cursor.fetchall()[0][0]}")

    # Connect
    print("Connect unlabeled and like-labelled lines")
    cursor.execute(f"""
      SELECT id, type FROM {args.table}_lines
      WHERE type LIKE '%00%' AND NOT ST_IsClosed(wkb_geometry) ORDER BY id
    """)
    lines = cursor.fetchall()
    deleted = []
    for line in lines:
        if line[0] in deleted:
            continue
        verbosity(args.verbose, f"connect {line[0]}")
        connect = shortest_connect(f"{args.table}_lines", cursor, line[0], line[1])
        while len(connect) > 0:
            verbosity(args.verbose, f"- with {connect[0][0]}")
            make_valid(f"{args.table}_lines", cursor, connect[0][2:], line[0])
            if line[0] == connect[0][0]:
                break
            verbosity(args.verbose, f"- remove {connect[0][0]}")
            cursor.execute(f"""
              DELETE FROM {args.table}_lines WHERE id = {connect[0][0]}
            """)
            deleted.append(connect[0][0])
            connect = shortest_connect(f"{args.table}_lines", cursor, line[0], line[1])

    # Closed non-labelled
    cursor.execute(f"""
      SELECT count(*) FROM {args.table}_lines WHERE type LIKE '%CONTOURS%'
    """)
    print(f"Remaining lines: {cursor.fetchall()[0][0]}")

    handle_unlabeled_rings(args, cursor)

    # Convert to polygons
    print("Turn closed lines into polygons")
    cursor.execute(f"""
      INSERT INTO {args.table}_polys (id, name, type, wkb_geometry)
      SELECT nextval('serial'), 'elevation', type, ST_MakePolygon(wkb_geometry)
      FROM {args.table}_lines
      WHERE ST_IsClosed(wkb_geometry)
    """)

    # Rest
    cursor.execute(f"""
      SELECT count(*) FROM {args.table}_lines WHERE type LIKE '%CONTOURS%'
    """)
    print(f"Remaining lines: {cursor.fetchall()[0][0]}")
    conn.commit()

if __name__ == '__main__':
    main()
