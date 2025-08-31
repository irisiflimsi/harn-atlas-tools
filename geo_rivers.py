#!/usr/bin/python
"""
Detect all rivers.
"""
import sys
import argparse
import psycopg2

EPS0 = 0.005 # must be a bit bigger than EPSB from geo_coast and EPS1
EPS1 = 0.004 # area rivers grow/shrink to make semi-valid

def make_axis(verbose, table, cursor, merge, bound):
    """Removes the smallest segments until a single line remains. Update."""
    sql_array = "'" + "'::geometry, '".join(merge) + "'::geometry"
    cursor.execute(f"""
        WITH lines (geo) AS (
          SELECT (ST_Dump(ST_Union(ARRAY[{sql_array}]))).geom)
        SELECT (ST_Dump(ST_LineMerge(ST_Union(geo)))).geom FROM lines
        WHERE ST_Covers(ST_Buffer('{bound[1]}'::geometry, -{EPS1/50}), geo) """)
    merge = cursor.fetchall()
    if verbose:
        print(f"- Create axis for {bound[0]} with {len(merge)} medial(s)")
    for axis in merge:
        sql_array = "'" + "'::geometry, '".join(axis) + "'::geometry"
        cursor.execute(f"""
            INSERT INTO {table} (id, name, type, wkb_geometry)
            VALUES (nextval('serial'), 'candidate', 'STREAMS', ST_Union(ARRAY[{sql_array}]))""")

def handle_lakes(args, cursor, vertex, level, lakes):
    """Add lakes to river network."""
    other_vertex = 'end' if vertex == 'start' else 'start'
    if args.verbose:
        print(f"Handle lakes level {level} for {vertex}")
    rm_lakes = []
    for lake in lakes:
        if args.verbose:
            print(f"- lake {lake[0]}")
        cursor.execute(f"""
            SELECT id FROM {args.table}_lines
            WHERE type LIKE 'River/{level}/Mouth:{other_vertex}' AND
              ST_Distance(ST_MakePolygon('{lake[1]}'::geometry),
                ST_{vertex.capitalize()}Point(wkb_geometry)) < {EPS0}""")
        lines = cursor.fetchall() # lines with v in lake and ov connected
        for pts in lines:
            if args.verbose:
                print(f"- line {pts[0]} in lake {lake[0]}")
            cursor.execute(f"""
                WITH lines(geo) AS (
                  SELECT (ST_Dump(
                    ST_Difference(ST_MakeValid(wkb_geometry),
                      ST_MakeValid(ST_MakePolygon('{lake[1]}'::geometry))))).geom
                  FROM {args.table}_lines
                  WHERE id = {pts[0]})
                SELECT lines.geo FROM lines ORDER BY ST_Length(lines.geo) DESC
                LIMIT 1""")
            line = cursor.fetchall()
            if len(line) > 0:
                cursor.execute(f"""
                    UPDATE {args.table}_lines SET wkb_geometry = '{line[0][0]}'::geometry
                    WHERE id = {pts[0]}""")
            # Lake successfully considered
            if len(pts) > 0:
                rm_lakes.append(lake)
        if len(lines) > 0:
            handle_river(args, cursor, vertex, level + 1, lake[1])
            handle_river(args, cursor, other_vertex, level + 1, lake[1])
    # Remove lakes considered
    for lake in rm_lakes:
        if lake in lakes:
            lakes.remove(lake)

def handle_river(args, cursor, vertex, level, old):
    """Creates rivers for all lines. Update."""
    idx = 0 if vertex == 'start' else -1
    if args.verbose:
        print(f"Handle outflows level {level} for {vertex}")
    if f"{old}" == "None":
        return 0
    obj = f"'{old}'::geometry"
    cursor.execute(f"""
        SELECT id, name, type FROM {args.table}_lines
        WHERE name = 'candidate' AND type NOT LIKE 'River/%' AND
          ST_Distance({obj},ST_{vertex.capitalize()}Point(wkb_geometry))
          < {EPS0}""")
    lines = cursor.fetchall()
    if args.verbose:
        print(f"Shift {len(lines)} river {vertex}s")
    for pts in lines:
        if args.verbose:
            print(f"- line {pts} with {vertex}")
        while True:
            cursor.execute(f"""
                SELECT ST_Distance('{old}'::geometry,
                  ST_{vertex.capitalize()}Point(wkb_geometry)),
                  ST_NPoints(wkb_geometry)
                FROM {args.table}_lines
                WHERE id = {pts[0]}""")
            line = cursor.fetchall()
            if args.verbose:
                print(f"- - shorten {pts[0]}: {line[0][1]}")
            cursor.execute(f"""
                SELECT ST_Intersects('{old}'::geometry, wkb_geometry)
                FROM {args.table}_lines WHERE id = {pts[0]}""")
            intersects = cursor.fetchall()[0]
            if not intersects[0] or line[0][1] == 2:
                break
            cursor.execute(f"""
                UPDATE {args.table}_lines SET wkb_geometry =
                  ST_RemovePoint(wkb_geometry, {idx} * (1 - ST_NPoints(wkb_geometry)))
                WHERE id = {pts[0]}""")
        cursor.execute(f"""
            SELECT wkb_geometry,
              ST_ClosestPoint('{old}'::geometry, ST_{vertex.capitalize()}Point(wkb_geometry))
            FROM {args.table}_lines
            WHERE id = {pts[0]}""")
        pts2 = cursor.fetchall()
        if len(pts2) > 0:
            cursor.execute(f"""
                INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
                VALUES (nextval('serial'), '-', 'River/{level}/Mouth:{vertex}',
                  ST_SetPoint(ST_RemoveRepeatedPoints('{pts2[0][0]}'::geometry), {idx},
                    '{pts2[0][1]}'::geometry))""")
            cursor.execute(f"""
                DELETE FROM {args.table}_lines WHERE id = {pts[0]}""")
    return len(lines)

def execute(args, cursor):
    """Actual main method. Iterates through levels."""
    # Initialize
    cursor.execute(f"""
        SELECT count(*) FROM {args.table}_lines
        WHERE type LIKE '%STREAMS%' AND (NOT ST_IsClosed(wkb_geometry) OR
          ST_IsClosed(wkb_geometry) AND style LIKE '%fill: #36868d%')""")
    print(f"Found {cursor.fetchall()[0][0]} rivers")

    # These are all extended rivers
    # (Buffer because there are strange duplicates)
    cursor.execute(f"""
        SELECT t1.id, ST_Buffer(ST_MakeValid(
          ST_MakePolygon(ST_AddPoint(t1.geo, ST_StartPoint(t1.geo)))), {EPS1}/100)
        FROM (
          SELECT id, wkb_geometry FROM {args.table}_lines
          WHERE type LIKE '%STREAMS%' AND 
            style LIKE '%fill: #36868d%') AS t1 (id,geo)""")
    rows = cursor.fetchall()
    print(f"Thinning area rivers: {len(rows)}")
    for row in rows:
        cursor.execute(f"""
            SELECT CG_ApproximateMedialAxis('{row[1]}'::geometry)""")
        axis = cursor.fetchall()
        make_axis(args.verbose, f"{args.table}_lines", cursor, [l[0] for l in axis], row)

    cursor.execute(f"""
        UPDATE {args.table}_lines SET name = 'candidate'
        WHERE type LIKE '%STREAMS%' AND NOT ST_IsClosed(wkb_geometry)""")

    # Shores
    cursor.execute(f"""
        SELECT ST_Union(wkb_geometry) FROM {args.table}_lines WHERE type = '0'""")
    old_term = cursor.fetchall()[0][0]
    cursor.execute(f"""
        SELECT id, wkb_geometry FROM {args.table}_lines
        WHERE type = 'COASTLINE/tmp-lake' OR type LIKE 'Lake%'""")
    lakes = list(cursor.fetchall())
    length = handle_river(args, cursor, "start", 0, old_term)
    length += handle_river(args, cursor, "end", 0, old_term)
    handle_lakes(args, cursor, "start", 0, lakes)
    handle_lakes(args, cursor, "end", 0, lakes)

    # Recurse rivers into rivers
    level = 0
    while length > 0:
        level = level + 1
        cursor.execute(f"""
            SELECT ST_Union(wkb_geometry) FROM {args.table}_lines
            WHERE type LIKE 'River/{level-1}/%'""")
        old_term = cursor.fetchall()[0][0]
        length = handle_river(args, cursor, "start", level, old_term)
        length += handle_river(args, cursor, "end", level, old_term)
        handle_lakes(args, cursor, "start", level, lakes)
        handle_lakes(args, cursor, "end", level, lakes)

    cursor.execute(f"""
        SELECT count(*) FROM {args.table}_lines
        WHERE type LIKE '%STREAMS%' AND NOT ST_IsClosed(wkb_geometry) AND NOT name = '-'""")

    print(f"Leave {cursor.fetchall()[0][0]} rivers")

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Create rivers from postgis database.')
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
        CREATE TEMP SEQUENCE IF NOT EXISTS serial START 500000""")

    if args.test:
        for typ in ["Lake/test", "COASTLINE/tmp-lake"]:
            test_unit1(args, typ, cursor)
            test_unit2(args, typ, cursor)
            test_unit3(args, typ, cursor)
        test_sample_area1(args, cursor)
        test_sample_lake_river(args, cursor)
        test_sample_area2(args, cursor)
    else:
        execute(args, cursor)
        conn.commit()

def test_unit1(args, typ, cursor):
    """Simple tests with artificial rivers and lakes. Lakes and coast."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '-', '0',
            'LINESTRING(10 10, 30 10, 30 20, 10 20, 10 10)'::geometry)""")

    if args.verbose:
        print(f"Prime with {typ}")
        print("1. 0/Mouth:start")                                   # <-
        print("2. 0/Mouth:end")                                     # ->
        print("3. 1/Mouth:start at 0/Mouth:start")                  # <-O<-
        print("4. 1/Mouth:end at 0/Mouth:start")                    # <-O->
        print("5. 1/Mouth:start at 0/Mouth:end")                    # ->O<-
        print("6. 1/Mouth:end at 0/Mouth:end")                      # ->O->

    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '1', 'STREAMS',
            'LINESTRING(10.1000 10.0001, 10.1000 10.0999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '2', 'STREAMS',
            'LINESTRING(10.2000 10.0999, 10.2000 10.0001)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '3a', 'STREAMS',
            'LINESTRING(10.3000 10.0001, 10.3000 10.0999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '3b', '{typ}',
            'LINESTRING(10.3000 10.1000, 10.3100 10.1100,
              10.3000 10.1200, 10.2900 10.1100, 10.3000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '3c', 'STREAMS',
            'LINESTRING(10.3000 10.1201, 10.3000 10.1999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '4a', 'STREAMS',
            'LINESTRING(10.4000 10.0001, 10.4000 10.0999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '4b', '{typ}',
            'LINESTRING(10.4000 10.1000, 10.4100 10.1100,
              10.4000 10.1200, 10.3900 10.1100, 10.4000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '4c', 'STREAMS',
            'LINESTRING(10.4000 10.1999, 10.4000 10.1201)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '5a', 'STREAMS',
            'LINESTRING(10.5000 10.0999, 10.5000 10.0001)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '5b', '{typ}',
            'LINESTRING(10.5000 10.1000, 10.5100 10.1100,
              10.5000 10.1200, 10.4900 10.1100, 10.5000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '5c', 'STREAMS',
            'LINESTRING(10.5000 10.1201, 10.5000 10.1999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '6a', 'STREAMS',
            'LINESTRING(10.6000 10.0999, 10.6000 10.0001)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '6b', '{typ}',
            'LINESTRING(10.6000 10.1000, 10.6100 10.1100,
              10.6000 10.1200, 10.5900 10.1100, 10.6000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '6c', 'STREAMS',
            'LINESTRING(10.6000 10.1999, 10.6000 10.1201)'::geometry)""")

    # Test object
    execute(args, cursor)
    sel = f"SELECT count(*) FROM {args.table}_lines WHERE "
    num_tests = 0
    # 1
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.1 10)'::geometry AND
        type = 'River/0/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    # 2
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(10.2 10)'::geometry AND
        type = 'River/0/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    # 3
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.3 10)'::geometry AND
        type = 'River/0/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.3 10.12)'::geometry AND
        type = 'River/1/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    # 4
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.4 10)'::geometry AND
        type = 'River/0/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(10.4 10.12)'::geometry AND
        type = 'River/1/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    # 5
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(10.5 10)'::geometry AND
        type = 'River/0/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.5 10.12)'::geometry AND
        type = 'River/1/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    # 6
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(10.6 10)'::geometry AND
        type = 'River/0/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(10.6 10.12)'::geometry AND
        type = 'River/1/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1

    print(f"> {num_tests} tests passed")

def test_unit2(args, typ, cursor):
    """Simple tests with multiple artificial rivers and lakes. Lakes and coast."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '-', '0',
            'LINESTRING(10 10, 30 10, 30 20, 10 20, 10 10)'::geometry)""")

    if args.verbose:
        print("7. 2/Mouth:start at 1/Mouth:start at 0/Mouth:start") # <-O<-O<-
        print("8. 2/Mouth:end at 1/Mouth:start at 0/Mouth:start")   # <-O<-O->
        print("9. 2/Mouth:end at 1/Mouth:end at 0/Mouth:start")     # <-O->O->

    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '7a', 'STREAMS',
            'LINESTRING(10.7000 10.0001, 10.7000 10.0999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '7b', '{typ}',
            'LINESTRING(10.7000 10.1000, 10.7100 10.1100,
              10.7000 10.1200, 10.6900 10.1100, 10.7000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '7c', 'STREAMS',
            'LINESTRING(10.7000 10.1201, 10.7000 10.1999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '7d', '{typ}',
            'LINESTRING(10.7000 10.2000, 10.7100 10.2100,
              10.7000 10.2200, 10.6900 10.2100, 10.7000 10.2000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '7e', 'STREAMS',
            'LINESTRING(10.7000 10.2201, 10.7000 10.2999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '8a', 'STREAMS',
            'LINESTRING(10.8000 10.0001, 10.8000 10.0999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '8b', '{typ}',
            'LINESTRING(10.8000 10.1000, 10.8100 10.1100,
              10.8000 10.1200, 10.7900 10.1100, 10.8000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '8c', 'STREAMS',
            'LINESTRING(10.8000 10.1201, 10.8000 10.1999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '8d', '{typ}',
            'LINESTRING(10.8000 10.2000, 10.8100 10.2100,
              10.8000 10.2200, 10.7900 10.2100, 10.8000 10.2000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '8e', 'STREAMS',
            'LINESTRING(10.8000 10.2999, 10.8000 10.2201)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '9a', 'STREAMS',
            'LINESTRING(10.9000 10.0001, 10.9000 10.0999)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '9b', '{typ}',
            'LINESTRING(10.9000 10.1000, 10.9100 10.1100,
              10.9000 10.1200, 10.8900 10.1100, 10.9000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '9c', 'STREAMS',
            'LINESTRING(10.9000 10.1999, 10.9000 10.1201)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '9d', '{typ}',
            'LINESTRING(10.9000 10.2000, 10.9100 10.2100,
             10.9000 10.2200, 10.8900 10.2100, 10.9000 10.2000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '9e', 'STREAMS',
            'LINESTRING(10.9000 10.2999, 10.9000 10.2201)'::geometry)""")

    # Test object
    execute(args, cursor)
    sel = f"SELECT count(*) FROM {args.table}_lines WHERE "
    num_tests = 0
    # 7
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.7 10)'::geometry AND
        type = 'River/0/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.7 10.12)'::geometry AND
        type = 'River/1/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.7 10.22)'::geometry AND
        type = 'River/2/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    # 8
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.8 10)'::geometry AND
        type = 'River/0/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.8 10.12)'::geometry AND
        type = 'River/1/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(10.8 10.22)'::geometry AND
        type = 'River/2/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    # 9
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(10.9 10)'::geometry AND
        type = 'River/0/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(10.9 10.12)'::geometry AND
        type = 'River/1/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(10.9 10.22)'::geometry AND
        type = 'River/2/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1

    print(f"> {num_tests} tests passed")

def test_unit3(args, typ, cursor):
    """Tests with artificial long rivers and lakes. Lakes and coast."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '-', '0',
            'LINESTRING(10 10, 30 10, 30 20, 10 20, 10 10)'::geometry)""")

    if args.verbose:
        print("10. 2 x 1/Mouth:end at 0/Mouth:end")                 # ->O->->
        print("11. 1/Mouth:start and 1/Mouth:end at 0/Mouth:end")   # ->O-><-

    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '10a', 'STREAMS',
            'LINESTRING(11.0000 10.0999, 11.0000 10.0001)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '10b', '{typ}',
            'LINESTRING(11.0000 10.1000, 11.0100 10.1100,
              11.0000 10.1200, 10.9900 10.1100, 11.0000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '10c', 'STREAMS',
            'LINESTRING(10.9800 10.1999, 11.0000 10.1201)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '10d', 'STREAMS',
            'LINESTRING(11.0200 10.1999, 11.0000 10.1201)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '11a', 'STREAMS',
            'LINESTRING(11.1000 10.0999, 11.1000 10.0001)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '11b', '{typ}',
            'LINESTRING(11.1000 10.1000, 11.1100 10.1100,
              11.1000 10.1200, 11.0900 10.1100, 11.1000 10.1000)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '11c', 'STREAMS',
            'LINESTRING(11.0800 10.1999, 11.1000 10.1201)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, wkb_geometry)
          VALUES (nextval('serial'), '11d', 'STREAMS',
            'LINESTRING(11.1000 10.1201, 11.1200 10.1999)'::geometry)""")

    # Test object
    execute(args, cursor)
    sel = f"SELECT count(*) FROM {args.table}_lines WHERE "
    num_tests = 0
    # 10
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(11.0 10)'::geometry AND
        type = 'River/0/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(11.0 10.12)'::geometry AND
        type = 'River/1/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 2
    # 11
    num_tests += 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(11.1 10)'::geometry AND
        type = 'River/0/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_EndPoint(wkb_geometry) = 'POINT(11.1 10.12)'::geometry AND
        type = 'River/1/Mouth:end'""")
    assert cursor.fetchall()[0][0] == 1
    cursor.execute(f"""{sel}
        ST_StartPoint(wkb_geometry) = 'POINT(11.1 10.12)'::geometry AND
        type = 'River/1/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1

    print(f"> {num_tests} tests passed")

def test_sample_area1(args, cursor):
    """Check reduction to axis."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, style, wkb_geometry)
          VALUES (17573, '-', '/STREAMS-LAKES/Melderyn/-', 'fill: #36868d',
            'LINESTRING(-15.8033738 41.1343803, -15.8035777 41.1338582, -15.8034282 41.1344368,
              -15.8034282 41.1344650, -15.8034282 41.1344086, -15.8032107 41.1339852,
              -15.8022320 41.1319532, -15.8014571 41.1296954, -15.7997444 41.1287359,
              -15.7988735 41.1275422, -15.7980588 41.1262946, -15.7970644 41.1253078,
              -15.7961682 41.1242331, -15.7953741 41.1230776, -15.7946858 41.1218484,
              -15.7940624 41.1204385, -15.7938045 41.1190199, -15.7937053 41.1176116,
              -15.7937881 41.1162070, -15.7942799 41.1141591, -15.7949933 41.1128741,
              -15.7968083 41.1108994, -15.7978064 41.1099376, -15.7988129 41.1089460,
              -15.7998394 41.1079595, -15.8008977 41.1070127, -15.8019996 41.1061405,
              -15.8031569 41.1053777, -15.8043813 41.1047590, -15.8062556 41.1041966,
              -15.8073344 41.1034515, -15.8090965 41.1029266, -15.8102787 41.1034836,
              -15.8118288 41.1041684, -15.8130043 41.1050153, -15.8148465 41.1060311,
              -15.8162378 41.1059698, -15.8175929 41.1057020, -15.8189058 41.1052559,
              -15.8201705 41.1046597, -15.8213810 41.1039418, -15.8231655 41.1026303,
              -15.8242487 41.1016335, -15.8253676 41.0998927, -15.8253992 41.0986267,
              -15.8247967 41.0962803, -15.8246551 41.0948231, -15.8241714 41.0923856,
              -15.8240864 41.0909506, -15.8241578 41.0883216, -15.8244786 41.0868557,
              -15.8255987 41.0845116, -15.8263628 41.0831995, -15.8272582 41.0820511,
              -15.8281683 41.0809786, -15.8289768 41.0798941, -15.8297447 41.0780487,
              -15.8301596 41.0766062, -15.8307729 41.0751491, -15.8315735 41.0737599,
              -15.8325506 41.0725212, -15.8336931 41.0715157, -15.8349901 41.0708260,
              -15.8379550 41.0707109, -15.8392948 41.0710935, -15.8405529 41.0716644,
              -15.8417833 41.0722766, -15.8430399 41.0727830, -15.8450642 41.0730251,
              -15.8466357 41.0729770, -15.8481111 41.0727415, -15.8494712 41.0723013,
              -15.8506967 41.0716389, -15.8517684 41.0707372, -15.8526670 41.0695788,
              -15.8536144 41.0674371, -15.8542437 41.0661521, -15.8551010 41.0650430,
              -15.8561145 41.0640651, -15.8572126 41.0631735, -15.8590109 41.0617785,
              -15.8595003 41.0618209, -15.8594459 41.0623430, -15.8583127 41.0632447,
              -15.8571813 41.0641229, -15.8561208 41.0650387, -15.8552003 41.0660529,
              -15.8542805 41.0677475, -15.8537495 41.0690908, -15.8530221 41.0703618,
              -15.8521095 41.0714991, -15.8510226 41.0724413, -15.8489383 41.0733920,
              -15.8475773 41.0736409, -15.8461809 41.0738255, -15.8447792 41.0738843,
              -15.8434023 41.0737556, -15.8412174 41.0729404, -15.8396720 41.0721535,
              -15.8381643 41.0716566, -15.8367146 41.0714899, -15.8353434 41.0716937,
              -15.8340712 41.0723082, -15.8319876 41.0747749, -15.8313876 41.0760290,
              -15.8309143 41.0773740, -15.8304638 41.0787394, -15.8299320 41.0800547,
              -15.8286436 41.0818869, -15.8277430 41.0829702, -15.8269017 41.0841553,
              -15.8261648 41.0854117, -15.8255770 41.0867085, -15.8251094 41.0884062,
              -15.8250877 41.0898020, -15.8251366 41.0922304, -15.8254380 41.0935883,
              -15.8257890 41.0961109, -15.8261247 41.0973499, -15.8263600 41.1002032,
              -15.8257088 41.1015439, -15.8238180 41.1035052, -15.8227163 41.1043660,
              -15.8215515 41.1051542, -15.8203296 41.1058469, -15.8190564 41.1064214,
              -15.8177376 41.1068547, -15.8163792 41.1071241, -15.8147921 41.1072023,
              -15.8134538 41.1068507, -15.8113530 41.1052549, -15.8095451 41.1044788,
              -15.8082092 41.1042605, -15.8066769 41.1053255, -15.8047875 41.1058758,
              -15.8035885 41.1065117, -15.8024637 41.1072931, -15.8013967 41.1081820,
              -15.8003707 41.1091410, -15.7993693 41.1101321, -15.7976103 41.1118448,
              -15.7965777 41.1128872, -15.7953810 41.1146812, -15.7950383 41.1160622,
              -15.7949072 41.1175274, -15.7950094 41.1190046, -15.7959927 41.1216944,
              -15.7967626 41.1228760, -15.7975874 41.1240089, -15.7989152 41.1253492,
              -15.7997663 41.1265405, -15.8005328 41.1277058, -15.8021776 41.1285948,
              -15.8033194 41.1307114, -15.8037544 41.1324189, -15.8046923 41.1341687,
              -15.8045292 41.1347896, -15.8046651 41.1345073, -15.8033194 41.1343803,
              -15.8033194 41.1343803, -15.8033738 41.1343803)'::geometry)""")
    # Test object
    execute(args, cursor)
    cursor.execute(f"""
        SELECT count(*) FROM {args.table}_lines WHERE name = 'candidate'""")
    assert cursor.fetchall()[0][0] >= 1
    print("> 1 test passed")

def test_sample_lake_river(args, cursor):
    """Extracted real lake/river interaction that proved difficult at one time."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, style, wkb_geometry)
          VALUES (502241, '-', 'River/0/Mouth:start', '',
            'LINESTRING (-15.3383724 40.8395570, -15.339359 40.8375898, -15.340109 40.8363858,
              -15.341041 40.8354442, -15.342172 40.8347302, -15.343521 40.8342090,
              -15.345580 40.8337707, -15.346992 40.8337470, -15.348267 40.8339969,
              -15.350107 40.8344198, -15.351466 40.8344059, -15.352760 40.8339987,
              -15.355068 40.8325289, -15.356054 40.8315344, -15.356839 40.8304035,
              -15.358331 40.8281686, -15.359237 40.8270455, -15.360021 40.8259318,
              -15.360914 40.8240482, -15.361327 40.8223815, -15.361412 40.8208692,
              -15.361194 40.8194863, -15.360698 40.8182077, -15.359951 40.8170086,
              -15.358977 40.8158640, -15.357325 40.8143397, -15.356236 40.8134931,
              -15.355132 40.8126672, -15.354069 40.8117881, -15.352907 40.8105297,
              -15.352222 40.8093100, -15.351740 40.8078784, -15.351399 40.8064079,
              -15.351031 40.8045607)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, style, wkb_geometry)
          VALUES (17661, '-', 'Lake', '...;fill: #d4effc;...',
            'LINESTRING (-15.3460973 40.8023452, -15.3472528 40.8034318, -15.3485577 40.8051674,
              -15.3509637 40.8064092, -15.3524182 40.8056472, -15.3525695 40.8042425,
              -15.3523366 40.8026274, -15.3522851 40.8012088, -15.3523774 40.7993819,
              -15.3516977 40.7972229, -15.3519696 40.7956283, -15.3519424 40.7938503,
              -15.3513866 40.7925706, -15.3504200 40.7910281, -15.3496580 40.7898584,
              -15.3489047 40.7886828, -15.3480955 40.7874015, -15.3464915 40.7856517,
              -15.3454584 40.7845511, -15.3446293 40.7834363, -15.3433379 40.7825755,
              -15.3410678 40.7813337, -15.3397085 40.7827731, -15.3395796 40.7841877,
              -15.3395726 40.7855953, -15.3403523 40.7867915, -15.3416659 40.7874439,
              -15.3417067 40.7892219, -15.3411372 40.7905366, -15.3412853 40.7924110,
              -15.3416659 40.7938080, -15.3427806 40.7956283, -15.3434195 40.7972088,
              -15.3441399 40.7989303, -15.3448196 40.8005249, -15.3453497 40.8017949,
              -15.3461381 40.8023452, -15.3460973 40.8023452)'::geometry);
        INSERT INTO {args.table}_lines (id, name, type, style, wkb_geometry)
          VALUES (17551, 'candidate', '/STREAMS-LAKES/Melderyn/-', '...;fill: none;...',
            'LINESTRING (-15.3492374 40.7915079, -15.3505987 40.7907402, -15.3517798 40.7900334,
              -15.3527635 40.7892157, -15.3535326 40.7881153, -15.3540902 40.7864702,
              -15.3542228 40.7850310, -15.3541557 40.7836380, -15.3541581 40.7821522,
              -15.3545787 40.7807790, -15.3557349 40.7789207, -15.3562711 40.7776119,
              -15.3571486 40.7755764, -15.3581324 40.7746445, -15.3593164 40.7738612,
              -15.3607101 40.7728671, -15.3617628 40.7719262, -15.3628723 40.7710252,
              -15.3640535 40.7702500, -15.3653215 40.7696866, -15.3675475 40.7694380,
              -15.3689828 40.7697469, -15.3702387 40.7702408, -15.3714327 40.7707461,
              -15.3726824 40.7710893, -15.3746567 40.7709903, -15.3760502 40.7706249,
              -15.3774145 40.7701516, -15.3786661 40.7694986, -15.3797217 40.7685945,
              -15.3805290 40.7672932, -15.3808416 40.7663195, -15.3810703 40.7649313,
              -15.3812358 40.7631586, -15.3814941 40.7616064, -15.3820107 40.7602799,
              -15.3825000 40.7595179, -15.3833156 40.7585160, -15.3842400 40.7570908,
              -15.3851587 40.7560563, -15.3864421 40.7549600, -15.3874208 40.7541275,
              -15.3885490 40.7535207, -15.3896772 40.7531397, -15.3909006 40.7531820,
              -15.3932387 40.7541557, -15.3941479 40.7552424, -15.3948427 40.7565264,
              -15.3958099 40.7575027, -15.3974662 40.7589111, -15.3984465 40.7601665,
              -15.3991739 40.7614714, -15.3996715 40.7628198, -15.3999622 40.7642056,
              -15.4000690 40.7656229, -15.4000149 40.7670654, -15.3998228 40.7685273,
              -15.3995157 40.7700024, -15.3991166 40.7714847, -15.3986352 40.7730082,
              -15.3981996 40.7743269, -15.3977911 40.7756854, -15.3974443 40.7770733,
              -15.3971937 40.7784805, -15.3970856 40.7808681, -15.3973567 40.7822454,
              -15.3979036 40.7835599, -15.3990158 40.7857929, -15.3996315 40.7870692,
              -15.4003523 40.7882641, -15.4016801 40.7898992, -15.4025812 40.7909112,
              -15.4034486 40.7920031, -15.4043218 40.7931141, -15.4052399 40.7941832,
              -15.4070086 40.7957271, -15.4082747 40.7961711, -15.4106244 40.7961928,
              -15.4119845 40.7962589, -15.4146888 40.7962774, -15.4161296 40.7960105,
              -15.4174997 40.7956395, -15.4187909 40.7951428, -15.4199948 40.7944992,
              -15.4211031 40.7936870, -15.4228855 40.7916490)'::geometry)""")
    # Test object
    execute(args, cursor)
    cursor.execute(f"""
        SELECT count(*) FROM {args.table}_lines WHERE type = 'River/1/Mouth:start'""")
    assert cursor.fetchall()[0][0] == 1
    print("> 1 test passed")

def test_sample_area2(args, cursor):
    """Odditiy non-closed river."""
    # Priming test DB
    cursor.execute(f"""
        DELETE FROM {args.table}_lines""")
    cursor.execute(f"""
        INSERT INTO {args.table}_lines (id, name, type, style, wkb_geometry)
          VALUES (17609, 'non-closed area river', '/STREAMS-LAKES/Melderyn/-', 'fill: #36868d;',
            'LINESTRING (-16.0417992 40.8827364, -16.0453334 40.8814664, -16.0467063 40.8805492,
              -16.0472669 40.8792353, -16.0475497 40.8778140, -16.0473588 40.8751728,
              -16.0468911 40.8738824, -16.0462073 40.8726589, -16.0455004 40.8714260,
              -16.0449120 40.8699094, -16.0448501 40.8684552, -16.0452348 40.8670609,
              -16.0458516 40.8657344, -16.0466112 40.8642085, -16.0470261 40.8628610,
              -16.0478753 40.8604408, -16.0493570 40.8591567, -16.0510697 40.8579713,
              -16.0525142 40.8579235, -16.0545360 40.8573928, -16.0566837 40.8568848,
              -16.0583013 40.8577738, -16.0601772 40.8578867, -16.0613857 40.8586848,
              -16.0623521 40.8599610, -16.0634536 40.8607782, -16.0645950 40.8619225,
              -16.0656452 40.8629617, -16.0669724 40.8635772, -16.0684350 40.8637957,
              -16.0707935 40.8633477, -16.0718459 40.8625652, -16.0731316 40.8607794,
              -16.0739590 40.8596393, -16.0750754 40.8580701, -16.0751705 40.8579996,
              -16.0756871 40.8573363, -16.0751977 40.8599328, -16.0743429 40.8610568,
              -16.0735510 40.8622283, -16.0726858 40.8633153, -16.0712421 40.8643778,
              -16.0698494 40.8647940, -16.0683729 40.8648965, -16.0669063 40.8646820,
              -16.0655437 40.8641470, -16.0637930 40.8625857, -16.0617404 40.8607794,
              -16.0599597 40.8588744, -16.0580838 40.8587474, -16.0563983 40.8578302,
              -16.0551471 40.8581277, -16.0530136 40.8589027, -16.0512464 40.8588886,
              -16.0502387 40.8595159, -16.0484870 40.8610617, -16.0479534 40.8623071,
              -16.0473316 40.8645895, -16.0466812 40.8658871, -16.0457140 40.8680467,
              -16.0455868 40.8694730, -16.0459827 40.8707952, -16.0466645 40.8720700,
              -16.0473953 40.8733543, -16.0480113 40.8750317, -16.0482185 40.8765198,
              -16.0481884 40.8781303, -16.0478569 40.8796871, -16.0471603 40.8810139,
              -16.0455237 40.8821296, -16.0444665 40.8824954, -16.0419351 40.8833714,
              -16.0417584 40.8827787)'::geometry)""")
    # Test object
    execute(args, cursor)
    cursor.execute(f"""
        SELECT count(*) FROM {args.table}_lines WHERE name = 'candidate' AND type = 'STREAMS'""")
    assert cursor.fetchall()[0][0] >= 1
    print(f"> 1 test passed")

if __name__ == '__main__':
    main()
