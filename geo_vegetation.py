#!/usr/bin/python
"""
Connects all roads with either endpoints or the next closest road
if within reasonable distance.
"""
import sys
import argparse
import psycopg2

EPS = 0.00025 # grow to cover draw glitches

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Create vegetation areas from postgis database.')
    parser.add_argument(
        '-d', '--database', dest='db', required=True,
        help='db to connect to user:password@dbname:host')
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
        host=f"{args.db.split('@')[1].split(':')[1]}")
    cursor = conn.cursor()

    # Initialize
    types = ["WOODLAND", # default
#        "SWAMP",
        "CROPLAND",
        "HEATH",
        "FOREST",
        "NEEDLELEAF",
        "ALPINE",
        "SNOW_x2F_ICE",
        "SHOAL_x2F_REEF"]

    sql_area = "type LIKE '%" + "%' OR type LIKE '%".join(types) + "%'"
    # Initialize
    cursor.execute(f"""
        CREATE TEMP SEQUENCE IF NOT EXISTS serial START 300000;
        ALTER TABLE {args.table}_polys ALTER id SET NOT NULL;
        SELECT count(*) FROM {args.table}_lines WHERE {sql_area}""")
    print(f"Identifying areas: {cursor.fetchall()[0][0]}")
    redux = {}
    raw = {}
    for typ in types:
        print(f"Set up {typ}")
        if typ == "WOODLAND":
            cursor.execute(f"""
                SELECT ST_MakePolygon(wkb_geometry)
                FROM {args.table}_lines
                WHERE type = '0' AND ST_NPoints(wkb_geometry) > 3""")
            rows = list(cursor.fetchall())
            land = "'" + "'::geometry, '".join([row[0] for row in rows]) + "'::geometry"
            cursor.execute(f"""SELECT ST_Union(ARRAY[{land}])""")
            land_sql = cursor.fetchall()[0][0]
        elif typ == "SWAMP":
            rows = []
        else:
            cursor.execute(f"""
                SELECT ST_Buffer(ST_MakePolygon(ST_AddPoint(wkb_geometry, ST_StartPoint(wkb_geometry))), {EPS}, 2)
                FROM {args.table}_lines
                WHERE type LIKE '%{typ}%' AND ST_NPoints(wkb_geometry) > 3""")
            rows = list(cursor.fetchall())
        raw[typ] = "'" + "'::geometry, '".join([row[0] for row in rows]) + "'::geometry"
        print(f"Found {len(rows)}")

    for i in range(len(types)):
        print(f"Normalize {types[i]}")
        redux[types[i]] = raw[types[i]]
        for j in range(i+1,len(types) - 1):
            if args.verbose:
                print(f"- reduce {types[i]} by {types[j]}")
            cursor.execute(f"""
                SELECT ST_Difference(
                  ST_Union(ARRAY[{redux[types[i]]}]), ST_Union(ARRAY[{raw[types[j]]}]))""")
            redux_sql = list(cursor.fetchall())
            redux[types[i]] = "'" + "'::geometry, '".join([row[0] for row in redux_sql]) + "'::geometry"

        cursor.execute(f"""
            WITH ret AS (
              INSERT INTO {args.table}_polys (id, name, type, wkb_geometry)
              SELECT nextval('serial'), '-', 'VEGTMP/{types[i]}', tl.geo FROM (
                SELECT (ST_Dump(ST_Union(ARRAY[{redux[types[i]]}]))).geom)
              AS tl (geo)
              WHERE ST_GeometryType(tl.geo) = 'ST_Polygon' RETURNING id)
            SELECT * FROM ret""")
        if args.verbose:
            print(f"- normalized {len(cursor.fetchall())}")

    print(f"Restrict real vegetation to land")
    cursor.execute(f"""
        INSERT INTO {args.table}_polys (id, name, type, wkb_geometry)
        SELECT nextval('serial'), '-', 'VEG/' || tl.typ, tl.geo FROM (
          SELECT (ST_Dump(ST_Intersection(wkb_geometry, '{land_sql}'::geometry))).geom, substring(type, 7)
          FROM {args.table}_polys
          WHERE type LIKE '%VEGTMP/%' AND type NOT LIKE '%SHOAL%')
        AS tl (geo, typ)""")
    print(f"Restrict shoal/reef to off land")
    cursor.execute(f"""
        INSERT INTO {args.table}_polys (id, name, type, wkb_geometry)
        SELECT nextval('serial'), '-', 'VEG/' || tl.typ, tl.geo FROM (
          SELECT (ST_Dump(ST_Difference(wkb_geometry, '{land_sql}'::geometry))).geom, substring(type, 7)
          FROM {args.table}_polys
          WHERE type LIKE '%VEGTMP/%' AND type LIKE '%SHOAL%')
        AS tl (geo, typ)""")
    cursor.execute(f"""
        DELETE FROM {args.table}_polys
        WHERE type LIKE '%VEGTMP/%'""")

    conn.commit()

if __name__ == '__main__':
    main()
