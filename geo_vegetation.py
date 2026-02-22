#!/usr/bin/python
"""
Creates all vegetation (including shoal/reef).
"""
import sys
import argparse
import psycopg2

EPSG = 0.00025 # grow to cover draw glitches
EPSI = 0.01 # grow swamp
EPSD = 0.0125 # shrink swamp

def geo_array(rows):
    """Create a postgis geometry array."""
    return "'" + "'::geometry, '".join([row[0] for row in rows]) + "'::geometry"

def make_swamp(args, cursor):
    """Make Swamp out of various pieces."""

    # Areas as lines
    cursor.execute(f"""
      SELECT topring.id, ST_MakeValid(ST_MakePolygon(topring.wkb_geometry))
      FROM {args.table}_lines AS topring
      WHERE ST_IsClosed(topring.wkb_geometry) AND
        topring.type LIKE '%SWAMP%' AND
        NOT EXISTS (
          SELECT * FROM {args.table}_lines AS covers
          WHERE covers.type LIKE '%SWAMP%' AND
            topring.id <> covers.id AND
            CASE WHEN ST_IsClosed(covers.wkb_geometry) THEN
              ST_Covers(ST_MakePolygon(covers.wkb_geometry), topring.wkb_geometry)
            END
        )
    """)
    ret = []
    for poly in cursor.fetchall():
        cursor.execute(f"""
          SELECT ST_Union(ST_MakeValid(ST_MakePolygon(wkb_geometry)))
          FROM {args.table}_lines
          WHERE type LIKE '%SWAMP%' AND ST_NPoints(wkb_geometry) > 3 AND
            {poly[0]} <> id AND
            CASE WHEN ST_IsClosed(wkb_geometry) THEN
              ST_Covers('{poly[1]}'::geometry, ST_MakePolygon(wkb_geometry))
            END
        """)
        holes = cursor.fetchall()[0]
        if args.verbose:
            print(f"- swamp poly {poly[0]}")
        if holes[0] is not None:
            if args.verbose:
                print(f"- - with holes")
            cursor.execute(f"""
              SELECT ST_Difference('{poly[1]}'::geometry, '{holes[0]}'::geometry)
            """)
            ret.append([cursor.fetchall()[0][0]])
        else:
            ret.append([poly[1]])

    # Symbols on polys
    cursor.execute(f"""
      SELECT ST_Buffer(
        ST_Buffer(
          ST_Buffer(ST_Union(ST_MakeValid(wkb_geometry)), {EPSI}),
          -{EPSD}
        ),
        {EPSD}
      )
      FROM {args.table}_polys
      WHERE type LIKE '%SWAMP%'
    """)
    ret.append([cursor.fetchall()[0][0]])

    # Symbols on lines
    cursor.execute(f"""
      SELECT ST_Buffer(
        ST_Buffer(
          ST_Buffer(ST_Union(wkb_geometry), {EPSI}),
          -{EPSD}
        ),
        {EPSD}
      )
      FROM {args.table}_lines
      WHERE NOT ST_IsClosed(wkb_geometry) AND type LIKE '%SWAMP%'
    """)
    ret.append([cursor.fetchall()[0][0]])
    return ret

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Create vegetation areas from postgis database.')
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
    types = ["WOODLAND", # default
             "CROPLAND",
             "HEATH",
             "SWAMP",
             "FOREST",
             "NEEDLELEAF",
             "ALPINE",
             "SNOW/ICE",
             "SHOAL/REEF"]

    sql_area = "type LIKE '%" + "%' OR type LIKE '%".join(types) + "%'"
    # Initialize
    cursor.execute(f"""
      CREATE TEMP SEQUENCE IF NOT EXISTS serial START 300000;
      ALTER TABLE {args.table}_polys ALTER id SET NOT NULL;
      SELECT count(*) FROM {args.table}_lines WHERE {sql_area}
    """)
    print(f"Identifying areas: {cursor.fetchall()[0][0]}")
    redux = {}
    raw = {}
    for typ in types:
        print(f"Set up {typ}")
        if typ == "WOODLAND":
            cursor.execute(f"""
              SELECT ST_MakePolygon(wkb_geometry)
              FROM {args.table}_lines
              WHERE type = '0' AND ST_NPoints(wkb_geometry) > 3
            """)
            rows = list(cursor.fetchall())
            land = geo_array(rows)
            cursor.execute(f"""SELECT ST_Union(ARRAY[{land}])""")
            land_sql = cursor.fetchall()[0][0]
        elif typ == "SWAMP":
            rows = make_swamp(args, cursor)
        else:
            cursor.execute(f"""
              SELECT ST_Buffer(
                ST_MakePolygon(ST_AddPoint(wkb_geometry, ST_StartPoint(wkb_geometry))),
                {EPSG}, 2
              )
              FROM {args.table}_lines
              WHERE type LIKE '%{typ}%' AND ST_NPoints(wkb_geometry) > 3
            """)
            rows = list(cursor.fetchall())
        raw[typ] = geo_array(rows)
        print(f"Found {len(rows)}")

    for i, ty_i in enumerate(types):
        print(f"Normalize {ty_i}")
        redux[ty_i] = raw[ty_i]
        for j in range(i + 1, len(types) - 1):
            if args.verbose:
                print(f"- reduce {ty_i} by {types[j]}")
            cursor.execute(f"""
              SELECT ST_Union(ARRAY[{raw[types[j]]}])
            """)
            cursor.execute(f"""
              SELECT ST_Difference(
                ST_Union(ARRAY[{redux[ty_i]}]), ST_Union(ARRAY[{raw[types[j]]}])
              )
            """)
            redux[ty_i] = geo_array(list(cursor.fetchall()))

        cursor.execute(f"""
          WITH ret AS (
            INSERT INTO {args.table}_polys (id, name, type, wkb_geometry)
            SELECT nextval('serial'), '-', 'VEGTMP/{ty_i}', t1.geo FROM (
              SELECT (ST_Dump(ST_Union(ARRAY[{redux[ty_i]}]))).geom
            )
            AS t1 (geo)
            WHERE ST_GeometryType(t1.geo) = 'ST_Polygon' RETURNING id
          )
          SELECT * FROM ret
        """)
        if args.verbose:
            print(f"- normalized {len(cursor.fetchall())}")

    print(f"Restrict real vegetation to land")
    cursor.execute(f"""
      INSERT INTO {args.table}_polys (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'VEG/' || t1.typ, t1.geo FROM (
        SELECT (ST_Dump(ST_Intersection(wkb_geometry, '{land_sql}'::geometry))).geom, substring(type, 8)
        FROM {args.table}_polys
        WHERE type LIKE '%VEGTMP/%' AND type NOT LIKE '%SHOAL%'
      )
      AS t1 (geo, typ)
    """)
    print(f"Restrict shoal/reef to off land")
    cursor.execute(f"""
      INSERT INTO {args.table}_polys (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'VEG/' || t1.typ, t1.geo FROM (
        SELECT (ST_Dump(ST_Difference(wkb_geometry, '{land_sql}'::geometry))).geom, substring(type, 8)
        FROM {args.table}_polys
        WHERE type LIKE '%VEGTMP/%' AND type LIKE '%SHOAL%'
      )
      AS t1 (geo, typ)
    """)
    cursor.execute(f"""
      DELETE FROM {args.table}_polys
      WHERE type LIKE '%VEGTMP/%'
    """)

    conn.commit()

if __name__ == '__main__':
    main()
