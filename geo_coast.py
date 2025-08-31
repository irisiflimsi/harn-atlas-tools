#!/usr/bin/python
"""
Evaluates the coast lines on the map. Does not work well with partial
coast lines that are not closed.
"""
import sys
import argparse
import psycopg2

EPSL = 0.004 # distance considered connected
EPSB = 0.004 # buffer radius to weed out rivers

def shortest_connect(table, cursor, line_id):
    """
    Returns the id of the closest line, the type, the geometry of it,
    of the original line, and of the connecting line.
    """
    cursor.execute(f"""
        SELECT wkb_geometry FROM {table} WHERE id = {line_id}""")
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
            ORDER BY ST_Distance(pt1.p, pt2.p) ASC LIMIT 1) AS connect
          FROM {table} AS main)
          AS connects (add_id, add_type, add_geo, line_geo, connect_geo)
        WHERE ST_Length(connects.connect_geo) < {EPSL} AND
          (connects.add_type LIKE '%COASTLINE%' OR
            connects.add_type = '0')
        ORDER BY ST_Length(connects.connect_geo) ASC LIMIT 1""")
    ret = cursor.fetchall()
    return ret

def verbosity(verb, out):
    """Verbosity."""
    if verb:
        print(out)

def extract_lake(table, cursor, name, height, inner_point):
    """Extract a specific lake by inner point and update."""
    print(f"Special: {name}")
    cursor.execute(f"""
        UPDATE {table}
        SET type = {height}, name = 'Lake/{name}'
        WHERE ST_IsClosed(wkb_geometry) AND type LIKE '%COASTLINE%' AND
          ST_Covers(ST_MakePolygon(wkb_geometry), ST_GeomFromText('{inner_point}'))""")

def make_valid_polys(table, cursor, merge, line_id):
    """Removes the smallest segments until only disjoint polygons remain. Update."""
    multi_polys = True
    while multi_polys:
        sql_array = "'" + "'::geometry, '".join(merge) + "'::geometry"
        cursor.execute(f"""
            SELECT geo, ST_Length(geo) FROM (
              SELECT (ST_Dump(ST_LineMerge(ST_Union(ARRAY[{sql_array}])))).geom)
            AS lines (geo) ORDER BY ST_Length(geo) DESC""")
        merge = cursor.fetchall()
        if merge[-1][1] > EPSL:
            break
        merge = [m[0] for m in merge[:-1]]

    if len(merge) == 1:
        cursor.execute(f"""
            UPDATE {table}
            SET name = 'nameless',
              type = '/COASTLINE/tmp-lake',
              wkb_geometry = '{merge[0][0]}'::geometry
            WHERE id = {line_id}""")
    else:
        for poly in merge:
            cursor.execute(f"""
                INSERT INTO {table} (id, name, type, wkb_geometry)
                VALUES (
                  nextval('serial'), 'nameless', '/COASTLINE/tmp-lake',
                  '{poly[0]}'::geometry)""")
        cursor.execute(f"""
            DELETE FROM {table} WHERE id = {line_id}""")

def make_valid_line(table, cursor, merge, line_id):
    """Removes the smallest segments until a single line remains. Update."""
    multi_line = True
    while multi_line:
        sql_array = "'" + "'::geometry, '".join(merge) + "'::geometry"
        cursor.execute(f"""
            SELECT geo FROM (
              SELECT (ST_Dump(ST_LineMerge(ST_Union(ARRAY[{sql_array}])))).geom)
            AS lines (geo) ORDER BY ST_Length(geo) DESC""")
        merge = cursor.fetchall()
        if len(merge) == 1:
            break
        merge = [m[0] for m in merge[:-1]]

    cursor.execute(f"""
        UPDATE {table}
        SET wkb_geometry = '{merge[0][0]}'::geometry
        WHERE id = {line_id}""")

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Create coast lines from postgis database')
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
        CREATE TEMP SEQUENCE IF NOT EXISTS serial START 100000;
        SELECT id, wkb_geometry FROM {args.table}_lines WHERE type LIKE '%COASTLINE%'""")
    print(f"Identifying lines: {cursor.fetchall()[0][0]}")

    print("Validate lines")
    cursor.execute(f"""
        SELECT id, wkb_geometry FROM {args.table}_lines WHERE type LIKE '%COASTLINE%'""")
    lines = cursor.fetchall()
    for line in lines:
        make_valid_line(f"{args.table}_lines", cursor, [line[1]], line[0])

    # Connect
    print("Connect unlabeled and like-labelled lines")
    cursor.execute(f"""
        SELECT id FROM {args.table}_lines WHERE type LIKE '%COASTLINE%' AND NOT ST_IsClosed(wkb_geometry)
        ORDER BY id""")
    lines = cursor.fetchall()
    deleted = []

    for line in lines:
        if line[0] in deleted:
            continue
        verbosity(args.verbose, f"- connect {line[0]}")
        connect = shortest_connect(f"{args.table}_lines", cursor, line[0])
        while len(connect) > 0:
            verbosity(args.verbose, f"- - with {connect[0][0]}")
            make_valid_line(f"{args.table}_lines", cursor, connect[0][2:], line[0])
            if line[0] == connect[0][0]:
                break
            verbosity(args.verbose, f"- - remove {connect[0][0]}")
            cursor.execute(f"""
                DELETE FROM {args.table}_lines WHERE id = {connect[0][0]}""")
            deleted.append(connect[0][0])
            connect = shortest_connect(f"{args.table}_lines", cursor, line[0])

    # Islands
    print(f"Special: Melderyn Isle")
    # Make bigger to "overgrow" rivers than smaller to create union with reality => take boundary
    cursor.execute(f"""
        SELECT id, geo FROM (
          SELECT id, (ST_Dump(ST_Boundary(ST_Union(
                  ST_Buffer(ST_Buffer(ST_MakePolygon(wkb_geometry), {EPSB}), -2 * {EPSB}),
                      ST_MakePolygon(wkb_geometry))))).geom
          FROM {args.table}_lines
          WHERE ST_IsClosed(wkb_geometry) AND type LIKE '%COASTLINE%' AND
            ST_Covers(ST_MakePolygon(wkb_geometry), ST_GeomFromText('POINT(-15.3 40.33)')))
        AS lines (id, geo)""")
    poly = cursor.fetchall()
    cursor.execute(f"""
        SELECT wkb_geometry FROM {args.table}_lines WHERE id = {poly[0][0]}""")
    with_rivers = cursor.fetchall()[0][0]
    verbosity(args.verbose, f"- {poly[0][0]}")
    make_valid_line(f"{args.table}_lines", cursor, [p[1] for p in poly], poly[0][0])
    cursor.execute(f"""
        SELECT (ST_Dump(ST_Intersection(
          ST_Buffer(ST_MakePolygon(wkb_geometry), -{EPSB}),
          ST_Difference(ST_Buffer(ST_MakePolygon(wkb_geometry), {EPSB}),
            ST_MakePolygon('{with_rivers}'::geometry))))).geom
        FROM {args.table}_lines
        WHERE id = {poly[0][0]}""")
    for river in cursor.fetchall():
        print(f"- new area river")
        cursor.execute(f"""
            INSERT INTO {args.table}_lines (id, name, type, style, wkb_geometry)
            VALUES (
              nextval('serial'), 'temporary area river', '/STREAMS-LAKE/tmp-river', 'fill: #36868d',
              ST_ExteriorRing('{river[0]}'::geometry))""")

    # Lakes
    # Make smaller to "dry" rivers then bigger to create intersection with reality => take boundary
    cursor.execute(f"""
        SELECT id, geo FROM (
          SELECT id, (ST_Dump(ST_Boundary(ST_Intersection(
                  ST_Buffer(ST_Buffer(ST_MakePolygon(wkb_geometry), -{EPSB}), 2 * {EPSB}),
                      ST_MakePolygon(wkb_geometry))))).geom
          FROM {args.table}_lines
          WHERE ST_IsClosed(wkb_geometry) AND type LIKE '%COASTLINE%')
        AS lines (id, geo)
        WHERE NOT ST_IsEmpty(geo)""")
    poly = cursor.fetchall()
    print(f"Lake potential lines: {len(poly)}")
    make_valid_polys(f"{args.table}_lines", cursor, [p[1] for p in poly], poly[0][0])

    extract_lake(f"{args.table}_lines", cursor, "Arain", 4180, "POINT(-17.7 46.6)")
    extract_lake(f"{args.table}_lines", cursor, "Tontury", 520, "POINT(-17.8 45)")

    # All (non-distorted) closed is coast
    cursor.execute(f"""
        UPDATE {args.table}_lines
        SET type = '0'
        WHERE type LIKE '%COASTLINE%' AND ST_IsClosed(wkb_geometry)""")

    # Everything else must be main Harn.
    print(f"Remainder is Harn")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
        SELECT
          nextval('serial'), 'main', '0',
          ST_ExteriorRing(ST_Buffer(ST_MakePolygon(ST_ExteriorRing(tr.geo)), -{EPSB}))
        FROM (
          SELECT tl.geo FROM (
            SELECT (ST_Dump(ST_Buffer(ST_Union(wkb_geometry), {EPSB}))).geom
            FROM {args.table}_lines
            WHERE type LIKE '%COASTLINE%')
          AS tl (geo)
          ORDER BY ST_Length(tl.geo)
          ASC LIMIT 1)
        AS tr (geo)""")

    cursor.execute(f"""
        DELETE FROM {args.table}_lines AS tl
        USING (SELECT ST_MakePolygon(wkb_geometry) FROM {args.table}_lines WHERE name = 'main')
        AS tr (geo)
        WHERE tl.type = '0' AND tl.name <> 'main' AND ST_Covers(tr.geo, tl.wkb_geometry)""")

    cursor.execute(f"""
        SELECT count(*) FROM {args.table}_lines WHERE type LIKE '%COASTLINE%'""")
    print(f"Remaining lines: {cursor.fetchall()[0][0]}")
    conn.commit()

if __name__ == '__main__':
    main()
