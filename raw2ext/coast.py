#!/usr/bin/python
"""
Evaluates the coast lines on the map. Does not work well with partial
coast lines that are not closed.
"""
import inspect
import logging
from raw2ext import sql, shortest_connect

LOGGER = logging.getLogger(__name__)

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

def name_lake(lines, lake_id):
    """Detect a named lake."""
    sql(f"""
      UPDATE {lines} SET name = 'Lake/Arain', type = 'Lake/4180'
      WHERE id = {lake_id} AND
        ST_Covers(ST_MakePolygon(wkb_geometry), ST_GeomFromText('POINT(-17.7 46.6)'))
      RETURNING id
    """)
    if len(sql()) > 0:
        LOGGER.info("Found Arain")
    sql(f"""
      UPDATE {lines} SET name = 'Lake/Tontury', type = 'Lake/520'
      WHERE id = {lake_id} AND
        ST_Covers(ST_MakePolygon(wkb_geometry), ST_GeomFromText('POINT(-17.7 45.0)'))
      RETURNING id
    """)
    if len(sql()) > 0:
        LOGGER.info("Found Tontury")

def make_valid_lake(lines, geo, line_id):
    """Handles Lakes. Update."""
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (
        nextval('serial'), 'nameless', '/COASTLINE/tmp-lake', '{geo}'::geometry
      )
      RETURNING id
    """)
    lake_id = sql()[0][0]
    LOGGER.debug("- lake %s from %s", lake_id, line_id)
    name_lake(lines, lake_id)

def make_valid_line(lines, merge, line_id):
    """Removes the smallest segments until a single line remains. Update."""
    multi_line = True
    while multi_line:
        sql_array = "'" + "'::geometry, '".join(merge) + "'::geometry"
        sql(f"""
          SELECT geo FROM (
            SELECT (ST_Dump(ST_LineMerge(ST_Union(ARRAY[{sql_array}])))).geom
          )
          AS lines (geo)
          ORDER BY ST_Length(geo) DESC
        """)
        merge = sql()
        if len(merge) == 1:
            break
        merge = [m[0] for m in merge[:-1]]

    sql(f"UPDATE {lines} SET wkb_geometry = '{merge[0][0]}'::geometry WHERE id = {line_id}")

def encircle(lines, isle_id):
    """Make a valid polygon and extract rivers."""
    LOGGER.debug("- %s", isle_id)
    sql(f"""
      SELECT river.id, (
        ST_Dump(
          ST_Intersection(
            ST_Buffer(ST_MakePolygon(isle.wkb_geometry), -{EPS}),
            river.wkb_geometry
          )
        )
      ).geom
      FROM {lines} AS isle, {lines} AS river
      WHERE isle.id = {isle_id} AND (river.type LIKE '%COASTLINE%' OR river.type = '0') AND
        ST_Intersects(ST_MakePolygon(isle.wkb_geometry), river.wkb_geometry)
    """)
    for river in sql():
        sql(f"""
          INSERT INTO {lines} (id, name, type, style, wkb_geometry)
          VALUES (
            nextval('serial'), 'temporary area river',
            '/STREAMS-LAKE/tmp-river', 'fill: #36868d',
            ST_AddPoint('{river[1]}'::geometry, ST_StartPoint('{river[1]}'::geometry))
          )
          RETURNING id
        """)
        LOGGER.debug("- new area river: %s", sql()[0][0])
        sql(f"DELETE FROM {lines} WHERE id = {river[0]}")

def execute(lines, polygons):
    """Top-level work-horse function. Connecting, Islands, then Lakes."""
    sql(f"SELECT count(*) FROM {lines} WHERE type LIKE '%COASTLINE%'")
    LOGGER.debug("Identifying lines: %s", sql()[0][0])

    LOGGER.info("Remove fossils")
    sql(f"DELETE FROM {lines} WHERE style = 'fill: #d4effc;'")

    LOGGER.info("Validate lines")
    sql(f"SELECT id, wkb_geometry FROM {lines} WHERE type LIKE '%COASTLINE%'")
    rings = sql()
    # Consider self-intersecting lines
    for ring in rings:
        make_valid_line(lines, [ring[1]], ring[0])

    # Connect
    LOGGER.info("Connect unlabeled and like-labelled lines")
    sql(f"""
      SELECT id FROM {lines}
      WHERE type LIKE '%COASTLINE%' AND NOT ST_IsClosed(wkb_geometry)
      ORDER BY id
    """)
    rings = sql()
    deleted = []

    for ring in rings:
        if ring[0] in deleted:
            continue
        LOGGER.debug("- connect %s", ring[0])
        connect = shortest_connect(lines, ring[0], 0, EPS)
        while len(connect) > 0:
            LOGGER.debug("- - with %s", connect[0][0])
            make_valid_line(lines, connect[0][2:], ring[0])
            if ring[0] == connect[0][0]:
                break
            LOGGER.debug("- - remove %s", connect[0][0])
            sql(f"DELETE FROM {lines} WHERE id = {connect[0][0]}")
            deleted.append(connect[0][0])
            connect = shortest_connect(lines, ring[0], 0, EPS)

    # Islands
    LOGGER.info("Special: Melderyn Isle")
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), 'Coast/Melderyn', '0', geo FROM (
        SELECT (
          ST_Dump(
            ST_Boundary(
              ST_Union(
                ST_Buffer(ST_Buffer(ST_MakePolygon(wkb_geometry), {EPS}), -2 * {EPS}),
                ST_MakePolygon(wkb_geometry)
              )
            )
          )
        ).geom
        FROM {lines}
        WHERE ST_IsClosed(wkb_geometry) AND type LIKE '%COASTLINE%' AND
          ST_Covers(ST_MakePolygon(wkb_geometry), ST_GeomFromText('POINT(-15.3 40.33)'))
      )
      AS lines (geo)
      RETURNING id, style
    """)
    ids = sql()
    if len(ids) > 0:
        encircle(lines, ids[0][0])
    else:
        LOGGER.debug("- not found")

    LOGGER.info("Special: Harnic Isle")
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), 'Coast/Harn', '0', geo FROM (
        SELECT (
          ST_Dump(ST_Boundary(ST_Union(ST_Buffer(ST_Buffer(geo, {EPS}), -2 * {EPS}), geo)))
        ).geom
        AS geo
        FROM (
          SELECT ST_MakePolygon(ST_ExteriorRing((ST_Dump(geo)).geom)) AS geo FROM (
            SELECT ST_CollectionExtract(ST_Polygonize(geo), 3) AS geo FROM (
              SELECT ST_LineMerge(ST_MakeValid(ST_Union(wkb_geometry))) AS geo
              FROM {lines} WHERE type LIKE '%COASTLINE%' OR type = '0'
            )
          )
        )
        ORDER BY ST_Perimeter(geo) DESC
        LIMIT 1
      )
      ORDER BY ST_Perimeter(geo) DESC
      LIMIT 1
      RETURNING id
    """)
    ids = sql()
    if len(ids) > 0:
        encircle(lines, ids[0][0])
    else:
        LOGGER.debug("- not found")

    # All closed is coast
    sql(f"""
      UPDATE {lines} AS tinner
      SET type = '0', name = 'Coast/nameless'
      WHERE type LIKE '%COASTLINE%' AND ST_IsClosed(wkb_geometry) AND
        NOT EXISTS (
          SELECT wkb_geometry FROM {lines} AS touter
          WHERE ST_IsClosed(touter.wkb_geometry) AND (type = '0' OR type LIKE '%COASTLINE%') AND
            ST_Covers(ST_MakePolygon(touter.wkb_geometry), tinner.wkb_geometry) AND
            tinner.id <> touter.id
        )
    """)
    # Convert to polygons
    LOGGER.info("Turn closed lines into polygons")
    sql(f"""
      INSERT INTO {polygons} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), 'Coast/nameless', '0', ST_MakePolygon(wkb_geometry)
      FROM {lines}
      WHERE type = '0' AND ST_IsClosed(wkb_geometry)
      RETURNING type
    """)
    LOGGER.debug("Converted %s to polygons", len(sql()))

    # Lakes
    # Make smaller to "dry" rivers then bigger to create intersection with reality => take boundary
    sql(f"""
        SELECT id, geo FROM (
          SELECT id, (ST_Dump(ST_Boundary(ST_Intersection(
            ST_Buffer(ST_Buffer(ST_MakeValid(ST_MakePolygon(wkb_geometry)), -{EPS}), 2 * {EPS}),
              ST_MakeValid(ST_MakePolygon(wkb_geometry)))))).geom
          FROM {lines}
          WHERE ST_IsClosed(wkb_geometry) AND
            (type LIKE '%COASTLINE%' OR type = '0' OR type LIKE '%tmp-river%') AND
            name NOT LIKE '%Coast/%')
        AS lines (id, geo)
        WHERE NOT ST_IsEmpty(geo)""")
    polys = sql()
    LOGGER.info("Lake potential lines: %s", len(polys))
    for poly in polys:
        make_valid_lake(lines, poly[1], poly[0])

    sql(f"""
      DELETE FROM {lines} AS tinner
      USING (SELECT ST_MakePolygon(wkb_geometry) FROM {lines} WHERE name = 'main')
      AS touter (geo)
      WHERE tinner.type = '0' AND tinner.name <> 'main' AND ST_Covers(touter.geo, tinner.wkb_geometry)
    """)

    sql(f"SELECT count(*) FROM {lines} WHERE type LIKE '%COASTLINE%'")
    LOGGER.info("Remaining lines: %s", sql()[0][0])

def tests(inpre):
    """Test collector."""
    lines = f"{inpre}_lines"
    polygons = f"{inpre}_polygons"

    # Initialize
    sql("CREATE TEMP SEQUENCE IF NOT EXISTS serial START 100000")

    test_harnmain(lines, polygons)
    test_harnmelderyn(lines, polygons)
    test_harnlakes(lines, polygons)
    test_harnconnect(lines, polygons)

def main(inpre):
    """Main method."""
    lines = f"{inpre}_lines"
    polygons = f"{inpre}_polygons"

    # Initialize
    sql("CREATE TEMP SEQUENCE IF NOT EXISTS serial START 100000")

    execute(lines, polygons)

def test_harnmain(lines, polygons):
    """Test the pecularities of harn main."""
    # Priming test DB
    sql(f"DELETE FROM {lines}; DELETE FROM {polygons}")

    # Connected river inlet 1
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (
        nextval('serial'), 'connect-1', '/COASTLINE/test',
        'LINESTRING(-17.110 43.00, -17.155 43.00, -17.155 42.00, -17.160 42.00,
                    -17.160 43.00, -17.205 43.00, -17.205 42.00, -17.210 42.00,
                    -17.210 43.00, -17.305 43.00)'::geometry
      )
    """)
    # Connected river inlet 2
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (
        nextval('serial'), 'connect-2', '/COASTLINE/test',
        'LINESTRING(-17.310 43.00, -17.400 43.00, -17.400 42.00,
                    -17.410 42.00, -17.410 43.00, -17.505 43.00)'::geometry
      )
    """)
    # Small and close island
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (
        nextval('serial'), 'island-coast', '/COASTLINE/test',
        'LINESTRING(-17.510 43.00, -17.580 43.00, -17.580 42.97,
                    -17.620 42.97, -17.620 43.00, -17.650 43.00)'::geometry
      );
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (
        nextval('serial'), 'island-island', '/COASTLINE/test',
        'LINESTRING(-17.590 42.99, -17.610 42.99, -17.610 42.98, -17.590 42.98,
                    -17.590 42.99)'::geometry
      )
    """)
    # Fake closure
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (
        nextval('serial'), 'unconnect-1', '/COASTLINE/test',
        'LINESTRING(-17.105 43.00, -16.900 43.00, -16.900 41.00,
                    -17.650 41.00, -17.650 43.00)'::geometry
      )
    """)

    # Test object
    execute(lines, polygons)
    sel = f"SELECT count(*) FROM {lines} WHERE "
    num_tests = 0

    # Main
    num_tests += 1
    sql(f"""{sel}
        type = '0' AND ST_IsClosed(wkb_geometry) AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17 42)'::geometry)
    """)
    assert sql()[0][0] == 1

    # Small island
    num_tests += 1
    sql(f"""{sel}
        type = '0' AND ST_IsClosed(wkb_geometry) AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17.600 42.985)'::geometry)
    """)
    assert sql()[0][0] == 1

    # Connected -> area river
    num_tests += 1
    sql(f"""{sel}
        type LIKE '%tmp-river%'
    """)
    assert sql()[0][0] == 3

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def test_harnlakes(lines, polygons):
    """Test the fallout of extracting Arain & Tontury."""
    # Priming test DB
    sql(f"DELETE FROM {lines}")

    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (
        nextval('serial'), 'All', '/COASTLINE/test',
        'LINESTRING(-19.000 48, -16.000 48, -16.000 42, -17.695 42,
                    -17.695 44, -16.500 44, -16.500 45, -17.695 45,
                    -17.695 46, -16.500 46, -16.500 47, -17.900 47,
                    -17.900 46, -17.705 46, -17.705 45, -17.900 45,
                    -17.900 44, -17.705 44, -17.705 42, -19.000 42,
                    -19.000 48)'::geometry
      )
    """)

    # Test object
    execute(lines, polygons)
    sel = f"SELECT count(*) FROM {lines} WHERE "
    num_tests = 0

    sql(f"""{sel} name = 'Lake/Tontury' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17.7 45.0)'::geometry)
    """)
    num_tests += 1
    assert sql()[0][0] == 1

    sql(f"""{sel} name = 'Lake/Arain' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17.7 46.6)'::geometry)
    """)
    num_tests += 1
    assert sql()[0][0] == 1

    sql(f"""{sel} name = 'Coast/Harn' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17 43)'::geometry)
    """)
    num_tests += 1
    assert sql()[0][0] == 1

    sql(f"""{sel} type LIKE '%tmp-river'""")
    num_tests += 1
    assert sql()[0][0] == 1

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def test_harnmelderyn(lines, polygons):
    """Test that Harn and Melderyn are indeed different."""
    # Priming test DB
    sql(f"DELETE FROM {lines}")
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'melderyn', '/COASTLINE/melderyn',
        'LINESTRING(-15.2 40.2,-15.2 40.4,-15.4 40.4,-15.4 40.2,-15.2 40.2)'::geometry
      )
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'harn-0', '/COASTLINE/harn',
        'LINESTRING(-16 42,-16 44,-18 44,-18 42)'::geometry
      );
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'harn-0', '/COASTLINE/harn',
        'LINESTRING(-18 44,-18 42,-16 42,-16 44)'::geometry
      )
    """)

    # Test object
    execute(lines, polygons)

    sel = f"SELECT count(*) FROM {lines} WHERE "
    num_tests = 0

    # Melderyn
    sql(f"""{sel} name = 'Coast/Melderyn' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-15.3 40.33)'::geometry)
    """)
    num_tests += 1
    assert sql()[0][0] == 1

    # Harn
    sql(f"""{sel} name = 'Coast/Harn' AND
        ST_Contains(ST_MakePolygon(wkb_geometry), 'POINT(-17 43)'::geometry)
    """)
    num_tests += 1
    assert sql()[0][0] == 1

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def test_harnconnect(lines, polygons):
    """Test some weird redraws that are all over the map."""
    # Priming test DB
    sql(f"DELETE FROM {lines}")

    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'section-0', '/COASTLINE/test',
        'LINESTRING(-16.0001 41, -18.0001 41)'::geometry
      )
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'section-1', '/COASTLINE/test',
        'LINESTRING(-16.0002 41, -18.0002 41, -18.0003 43)'::geometry
      )
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'section-2', '/COASTLINE/test',
        'LINESTRING(-18.0003 41, -16.0004 41, -16.0005 43)'::geometry
      )
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'section-3', '/COASTLINE/test',
        'LINESTRING(-16.0004 43, -18.0006 43)'::geometry
      )
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'section-4', '/COASTLINE/test',
        'LINESTRING(-16.0005 43, -18.0007 43, -18.0008 41)'::geometry
      )
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'section-5', '/COASTLINE/test',
        'LINESTRING(-18.0006 43, -16.0009 43, -16.0010 41)'::geometry
      )
    """)

    # Test object
    execute(lines, polygons)

    sel = f"SELECT count(*) FROM {lines} WHERE "
    num_tests = 0

    # Main
    num_tests += 1
    sql(f"{sel} type = '0'")
    assert sql()[0][0] == 1

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")
