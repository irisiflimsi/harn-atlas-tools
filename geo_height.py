#!/usr/bin/python
"""
Creates some default heights.
"""
import sys
import argparse
import psycopg2

# Dump into xyz_work:
#   INSERT INTO xyz_work (id, wkb_geometry)
#     SELECT nextval('serial2'), (ST_Dump(CG_StraightSkeleton(ST_Difference(p3.wkb_geometry,ST_Buffer(p1.wkb_geometry, 0.001))))).geom
#     FROM xyz_polys AS p3, xyz_pts AS p1 WHERE p3.id = 406494 AND p1.id = 38750;
# This assumes that 38750 is the id of Mt Ardat at 1390ft in west Melderyn and its 1000ft elevation line in xyz_polys with id 406494.
# xyz_work is a work table (\d):
#  id           | integer              |              | not null      | nextval('xyz_work_id_seq'::regclass)
#  wkb_geometry | geometry(LineString) |              |               | 
# xyz_heights is the result table (\):
#  id           | integer |              | not null      | nextval('xyz_heights_id_seq'::regclass)
#  dist         | numeric |              |               | 
#  height       | numeric |              |               | 


# Wiggle room for peaks and points on elevation lines.
EPS = 0.0015 # 0.0015 * 100km = 150m

def execute(args, cursor):
    print("Extract points from Ardat and its 1000ft contour")
    cursor.execute(f"""
        INSERT INTO xyz_heights AS tr (id, wkb_geometry)
        SELECT nextval('serial'), geo FROM (
          SELECT (ST_Dump(ST_Union(geo))).geom AS geo FROM (
            SELECT (ST_DumpPoints(tl.wkb_geometry::geometry)).geom AS geo
            FROM xyz_work AS tl))""")

    print("Set peak(s) height")
    cursor.execute(f"""
        UPDATE xyz_heights AS tl SET dist = 0, height = 1390
        FROM xyz_pts AS tr
        WHERE tr.id = 38750 AND ST_Distance(tl.wkb_geometry, tr.wkb_geometry) < {EPS}""")

    print("Set 1000ft height")
    cursor.execute(f"""
        UPDATE xyz_heights AS tl SET height = 1000
         FROM xyz_lines AS tr
        WHERE tr.id = 8727 AND ST_Distance(tl.wkb_geometry, tr.wkb_geometry) < {EPS}""")

    print("Calculate distance from peak(s)")
    while True:
        cursor.execute(f"""
            UPDATE xyz_heights AS h SET dist = r.dist
            FROM (SELECT tr.id, len FROM xyz_heights AS tr, LATERAL (
              SELECT min(tl.dist + ST_Length(con.wkb_geometry)) AS len FROM xyz_work AS con, xyz_heights AS tl
              WHERE tl.dist IS NOT NULL AND (
                (ST_StartPoint(con.wkb_geometry) = tl.wkb_geometry AND ST_EndPoint(con.wkb_geometry) = tr.wkb_geometry) OR
                (ST_EndPoint(con.wkb_geometry) = tl.wkb_geometry AND ST_StartPoint(con.wkb_geometry) = tr.wkb_geometry))))
              AS r (id, dist)
            WHERE h.id = r.id AND h.dist IS NULL
            RETURNING h.id""")
        fixed = cursor.fetchall()
        verbosity(args.verbose, [p[0] for p in fixed])
        if len(fixed) == 0:
            break

    print("Calculate relative height: next dist / current dist = next height delta / current height delta")
    while True:
        cursor.execute(f"""
            UPDATE xyz_heights AS h SET height = r.height
            FROM (SELECT tr.id, len FROM xyz_heights AS tr, LATERAL (
              SELECT min(tl.height + ST_Length(con.wkb_geometry) / tl.dist * (1390 - tl.height)) AS len
              FROM xyz_work AS con, xyz_heights AS tl
              WHERE tl.height IS NOT NULL AND tl.height <> 1390 AND (
                (ST_StartPoint(con.wkb_geometry) = tl.wkb_geometry AND ST_EndPoint(con.wkb_geometry) = tr.wkb_geometry) OR
                (ST_EndPoint(con.wkb_geometry) = tl.wkb_geometry AND ST_StartPoint(con.wkb_geometry) = tr.wkb_geometry))))
              AS r (id, height)
            WHERE h.id = r.id AND h.height IS NULL
            RETURNING h.id
        """)
        fixed = cursor.fetchall()
        verbosity(args.verbose, [p[0] for p in fixed])
        if len(fixed) == 0:
            break
       
def verbosity(verb, out):
    """Verbosity."""
    if verb:
        print(f" - {out}")

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Create coast) lines from postgis database')
    parser.add_argument(
        '-d', '--database', dest='db', required=True,
        help='db to connect to user:password@dbname:host:port')
    parser.add_argument(
        '-t', '--table', dest='table', required=True,
        help='table prefix; _pts and _lines will be added')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='verbose', required=False)
    parser.add_argument(
        '-T', '--test', action='store_true', help='run tests instead',
        required=False)
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
        CREATE TEMP SEQUENCE IF NOT EXISTS serial""")

    execute(args, cursor)
    conn.commit()

if __name__ == '__main__':
    main()
