#!/usr/bin/python
"""
Evaluates the coast lines on the map. Does not work well with partial
coast lines that are not closed.
"""
import sys
import inspect
import argparse
import psycopg2

# This EPS value is used to grow the coast, thereby overgrowing rivers
# up to twice this width.  The coast is then shrunk by twice this
# width and "united" with the original coast.  This keeps an ocean
# inlet.  The cut-off part of the big islands (i.e. whatever old coast
# remains inside the new island) is turned into area rivers.  They
# should have a distance of 0 from the coast, which is important in
# geo_rivers later.
#
# It is also used as a GAP measure to connect non-connected coast
# lines; if the end distance is smaller then we assume that they were
# actually meant to be connected.

EPS = 0.006 # roughly 0.006 x 100km = 600m

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
        WHERE ST_Length(connects.connect_geo) < {EPS} AND
          (connects.add_type LIKE '%COASTLINE%' OR
            connects.add_type = '0')
        ORDER BY ST_Length(connects.connect_geo) ASC LIMIT 1""")
    ret = cursor.fetchall()
    return ret

def verbosity(verb, out):
    """Verbosity."""
    if verb:
        print(out)

def name_lake(args, cursor, lake_id):
    """Detect a named lake."""
    cursor.execute(f"""
        UPDATE {args.table}_lines SET name = 'Lake/Arain', type = 'Lake/4180'
        WHERE id = {lake_id} AND
          ST_Covers(ST_MakePolygon(wkb_geometry), ST_GeomFromText('POINT(-17.7 46.6)'))
        RETURNING id""")
    if len(cursor.fetchall()) > 0:
        print("Found Arain")
    cursor.execute(f"""
        UPDATE {args.table}_lines SET name = 'Lake/Tontury', type = 'Lake/520'
        WHERE id = {lake_id} AND
          ST_Covers(ST_MakePolygon(wkb_geometry), ST_GeomFromText('POINT(-17.7 45.0)'))
        RETURNING id""")
    if len(cursor.fetchall()) > 0:
        print("Found Tontury")

def make_valid_lake(args, cursor, geo, line_id):
    """Handles Lakes. Update."""
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (
            nextval('serial'), 'nameless', '/COASTLINE/tmp-lake', '{geo}'::geometry)
        RETURNING id""")
    lake_id = cursor.fetchall()[0][0]
    verbosity(args.verbose, f"- lake {lake_id} from {line_id}")
    name_lake(args, cursor, lake_id)

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

def encircle(args, cursor, isle_id):
    """Make a valid polygon and extract rivers."""
    verbosity(args.verbose, f"- {isle_id}")
    cursor.execute(f"""
        SELECT river.id, (ST_Dump(ST_Intersection(
          ST_Buffer(ST_MakePolygon(isle.wkb_geometry), -{EPS}),
          river.wkb_geometry))).geom
        FROM {args.table}_lines AS isle, {args.table}_lines AS river
        WHERE isle.id = {isle_id} AND (river.type LIKE '%COASTLINE%' OR river.type = '0') AND
          ST_Intersects(ST_MakePolygon(isle.wkb_geometry), river.wkb_geometry)""")
    for river in cursor.fetchall():
        cursor.execute(f"""
            INSERT INTO {args.table}_lines (id, name, type, style, wkb_geometry)
            VALUES (
              nextval('serial'), 'temporary area river',
              '/STREAMS-LAKE/tmp-river', 'fill: #36868d',
              ST_AddPoint('{river[1]}'::geometry, ST_StartPoint('{river[1]}'::geometry)))
            RETURNING id""")
        print(f"- new area river: {cursor.fetchall()[0][0]}")
        cursor.execute(f"""
            DELETE FROM {args.table}_lines WHERE id = {river[0]}""")

def execute(args, cursor):
    """Top-level work-horse function. Connecting, Islands, then Lakes."""
    cursor.execute(f"""
        SELECT count(*) FROM {args.table}_lines WHERE type LIKE '%COASTLINE%'""")
    print(f"Identifying lines: {cursor.fetchall()[0][0]}")

    print("Remove fossils")
    cursor.execute(f"""
        DELETE FROM {args.table}_lines WHERE style = 'fill: #d4effc;'""")

    print("Validate lines")
    cursor.execute(f"""
        SELECT id, wkb_geometry FROM {args.table}_lines WHERE type LIKE '%COASTLINE%'""")
    lines = cursor.fetchall()
    # Consider self-intersecting lines
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
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
        SELECT nextval('serial'), 'Coast/Melderyn', '0', geo FROM (
          SELECT (ST_Dump(ST_Boundary(ST_Union(
            ST_Buffer(ST_Buffer(ST_MakePolygon(wkb_geometry), {EPS}), -2 * {EPS}),
              ST_MakePolygon(wkb_geometry))))).geom
          FROM {args.table}_lines
          WHERE ST_IsClosed(wkb_geometry) AND type LIKE '%COASTLINE%' AND
            ST_Covers(ST_MakePolygon(wkb_geometry), ST_GeomFromText('POINT(-15.3 40.33)')))
        AS lines (geo)
        RETURNING id, style""")
    ids = cursor.fetchall()
    if len(ids) > 0:
        encircle(args, cursor, ids[0][0])
    else:
        print(f"- not found")

    print(f"Special: Harnic Isle")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
        SELECT nextval('serial'), 'Coast/Harn', '0', geo FROM (
          SELECT (ST_Dump(ST_Boundary(ST_Union(ST_Buffer(ST_Buffer(geo, {EPS}), -2 * {EPS}), geo)))).geom AS geo FROM (
            SELECT ST_MakePolygon(ST_ExteriorRing((ST_Dump(geo)).geom)) AS geo FROM (
              SELECT ST_CollectionExtract(ST_Polygonize(geo), 3) AS geo FROM (
                SELECT ST_LineMerge(ST_MakeValid(ST_Union(wkb_geometry))) AS geo
                FROM {args.table}_lines WHERE type LIKE '%COASTLINE%' OR type = '0')))
          ORDER BY ST_Perimeter(geo) DESC
          LIMIT 1)
        ORDER BY ST_Perimeter(geo) DESC
        LIMIT 1
        RETURNING id""")
    ids = cursor.fetchall()
    if len(ids) > 0:
        encircle(args, cursor, ids[0][0])
    else:
        print(f"- not found")

    # All closed is coast
    cursor.execute(f"""
        UPDATE {args.table}_lines AS tl
        SET type = '0', name = 'Coast/nameless'
        WHERE type LIKE '%COASTLINE%' AND ST_IsClosed(wkb_geometry) AND
          NOT EXISTS (
            SELECT wkb_geometry FROM {args.table}_lines AS tr
            WHERE ST_IsClosed(tr.wkb_geometry) AND (type = '0' OR type LIKE '%COASTLINE%') AND
              ST_Covers(ST_MakePolygon(tr.wkb_geometry), tl.wkb_geometry) AND tl.id <> tr.id)""")

    # Lakes
    # Make smaller to "dry" rivers then bigger to create intersection with reality => take boundary
    cursor.execute(f"""
        SELECT id, geo FROM (
          SELECT id, (ST_Dump(ST_Boundary(ST_Intersection(
            ST_Buffer(ST_Buffer(ST_MakeValid(ST_MakePolygon(wkb_geometry)), -{EPS}), 2 * {EPS}),
              ST_MakeValid(ST_MakePolygon(wkb_geometry)))))).geom
          FROM {args.table}_lines
          WHERE ST_IsClosed(wkb_geometry) AND (type LIKE '%COASTLINE%' OR type = '0' OR type LIKE '%tmp-river%') AND
            name NOT LIKE '%Coast/%')
        AS lines (id, geo)
        WHERE NOT ST_IsEmpty(geo)""")
    polys = cursor.fetchall()
    print(f"Lake potential lines: {len(polys)}")
    for poly in polys:
        make_valid_lake(args, cursor, poly[1], poly[0])

    cursor.execute(f"""
        DELETE FROM {args.table}_lines AS tl
        USING (SELECT ST_MakePolygon(wkb_geometry) FROM {args.table}_lines WHERE name = 'main')
        AS tr (geo)
        WHERE tl.type = '0' AND tl.name <> 'main' AND ST_Covers(tr.geo, tl.wkb_geometry)""")

    cursor.execute(f"""
        SELECT count(*) FROM {args.table}_lines WHERE type LIKE '%COASTLINE%'""")
    print(f"Remaining lines: {cursor.fetchall()[0][0]}")

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
        CREATE TEMP SEQUENCE IF NOT EXISTS serial START 100000""")

    if args.test:
        test_harnmain(args, cursor)
        test_harnmelderyn(args, cursor)
        test_harnlakes(args, cursor)
        test_harnconnect(args, cursor)
    else:
        execute(args, cursor)
        conn.commit()

def test_harnmain(args, cursor):
    """Test the pecularities of harn main."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    # Connected river inlet 1
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'connect-1', '/COASTLINE/test',
            'LINESTRING(-17.110 43.00, -17.155 43.00, -17.155 42.00, -17.160 42.00,
                        -17.160 43.00, -17.205 43.00, -17.205 42.00, -17.210 42.00,
                        -17.210 43.00, -17.305 43.00)'::geometry)""")
    # Connected river inlet 2
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'connect-2', '/COASTLINE/test',
            'LINESTRING(-17.310 43.00, -17.400 43.00, -17.400 42.00,
                        -17.410 42.00, -17.410 43.00, -17.505 43.00)'::geometry)""")
    # Small and close island
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'island-coast', '/COASTLINE/test',
            'LINESTRING(-17.510 43.00, -17.580 43.00, -17.580 42.97,
                        -17.620 42.97, -17.620 43.00, -17.650 43.00)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'island-island', '/COASTLINE/test',
            'LINESTRING(-17.590 42.99, -17.610 42.99, -17.610 42.98, -17.590 42.98,
                        -17.590 42.99)'::geometry)""")
    # Fake closure
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'unconnect-1', '/COASTLINE/test',
            'LINESTRING(-17.105 43.00, -16.900 43.00, -16.900 41.00,
                        -17.650 41.00, -17.650 43.00)'::geometry)""")

    # Test object
    execute(args, cursor)
    sel = f"SELECT count(*) FROM {args.table}_lines WHERE "
    num_tests = 0

    # Main
    num_tests += 1
    cursor.execute(f"""{sel}
        type = '0' AND ST_IsClosed(wkb_geometry) AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17 42)'::geometry)""")
    assert cursor.fetchall()[0][0] == 1

    # Small island
    num_tests += 1
    cursor.execute(f"""{sel}
        type = '0' AND ST_IsClosed(wkb_geometry) AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17.600 42.985)'::geometry)""")
    assert cursor.fetchall()[0][0] == 1

    # Connected -> area river
    num_tests += 1
    cursor.execute(f"""{sel}
        type LIKE '%tmp-river%'""")
    assert cursor.fetchall()[0][0] == 3

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def test_harnlakes(args, cursor):
    """Test the fallout of extracting Arain & Tontury."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'All', '/COASTLINE/test',
            'LINESTRING(-19.000 48, -16.000 48, -16.000 42, -17.695 42,
                        -17.695 44, -16.500 44, -16.500 45, -17.695 45,
                        -17.695 46, -16.500 46, -16.500 47, -17.900 47,
                        -17.900 46, -17.705 46, -17.705 45, -17.900 45,
                        -17.900 44, -17.705 44, -17.705 42, -19.000 42,
                        -19.000 48)'::geometry)""")

    # Test object
    execute(args, cursor)
    sel = f"SELECT count(*) FROM {args.table}_lines WHERE "
    num_tests = 0

    cursor.execute(f"""{sel} name = 'Lake/Tontury' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17.7 45.0)'::geometry)""")
    num_tests += 1
    assert cursor.fetchall()[0][0] == 1

    cursor.execute(f"""{sel} name = 'Lake/Arain' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17.7 46.6)'::geometry)""")
    num_tests += 1
    assert cursor.fetchall()[0][0] == 1

    cursor.execute(f"""{sel} name = 'Coast/Harn' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17 43)'::geometry)""")
    num_tests += 1
    assert cursor.fetchall()[0][0] == 1

    cursor.execute(f"""{sel} type LIKE '%tmp-river'""")
    num_tests += 1
    assert cursor.fetchall()[0][0] == 1

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def test_harnmelderyn(args, cursor):
    """Test that Harn and Melderyn are indeed different."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'melderyn', '/COASTLINE/melderyn',
            'LINESTRING(-15.2 40.2,-15.2 40.4,-15.4 40.4,-15.4 40.2,-15.2 40.2)'::geometry)""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'harn-0', '/COASTLINE/harn',
            'LINESTRING(-16 42,-16 44,-18 44,-18 42)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'harn-0', '/COASTLINE/harn',
            'LINESTRING(-18 44,-18 42,-16 42,-16 44)'::geometry)""")

    # Test object
    execute(args, cursor)

    sel = f"SELECT count(*) FROM {args.table}_lines WHERE "
    num_tests = 0

    # Melderyn
    cursor.execute(f"""{sel} name = 'Coast/Melderyn' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-15.3 40.33)'::geometry)""")
    num_tests += 1
    assert cursor.fetchall()[0][0] == 1

    # Harn
    cursor.execute(f"""{sel} name = 'Coast/Harn' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17 43)'::geometry)""")
    num_tests += 1
    assert cursor.fetchall()[0][0] == 1

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def test_harnconnect(args, cursor):
    """Test some weird redraws that are all over the map."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'section-0', '/COASTLINE/test',
            'LINESTRING(-16.0001 41, -18.0001 41)'::geometry)""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'section-1', '/COASTLINE/test',
            'LINESTRING(-16.0002 41, -18.0002 41, -18.0003 43)'::geometry)""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'section-2', '/COASTLINE/test',
            'LINESTRING(-18.0003 41, -16.0004 41, -16.0005 43)'::geometry)""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'section-3', '/COASTLINE/test',
            'LINESTRING(-16.0004 43, -18.0006 43)'::geometry)""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'section-4', '/COASTLINE/test',
            'LINESTRING(-16.0005 43, -18.0007 43, -18.0008 41)'::geometry)""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), 'section-5', '/COASTLINE/test',
            'LINESTRING(-18.0006 43, -16.0009 43, -16.0010 41)'::geometry)""")

    # Test object
    execute(args, cursor)

    sel = f"SELECT count(*) FROM {args.table}_lines WHERE "
    num_tests = 0

    # Main
    num_tests += 1
    cursor.execute(f"""{sel} type = '0'""")
    assert cursor.fetchall()[0][0] == 1

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

if __name__ == '__main__':
    main()
