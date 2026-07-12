#!/usr/bin/python
"""
Evaluates elevation labels when close to a contour and attachs the
elevation to the contour line in output as name. Includes heuristics
for connected lines and rings.
"""
import logging
import inspect
from raw2ext import sql, shortest_connect

LOGGER = logging.getLogger(__name__)

EPSP = 0.0025
EPSL = 0.007

def make_valid(lines, merge, line_id):
    """Removes the smallest segments until a single line remains. Update."""
    multi_line = True
    while multi_line:
        sql_array = "'" + "'::geometry, '".join(merge) + "'::geometry"
        sql(f"""
          SELECT geo
          FROM (SELECT (ST_Dump(ST_LineMerge(ST_Union(ARRAY[{sql_array}])))).geom)
          AS lines (geo) ORDER BY ST_Length(geo) DESC
        """)
        merge = sql()
        if len(merge) == 1:
            break
        merge = [m[0] for m in merge[:-1]]

    sql(f"UPDATE {lines} SET wkb_geometry = '{merge[0][0]}'::geometry WHERE id = {line_id}")

def sort_elevation_pts(table):
    """Sort all elevation points to their elevation."""
    sql(f"""
      SELECT substring(type, '[^1-9]([1-9][05]|5)00') AS elev, ST_Union(wkb_geometry)
      FROM {table}
      WHERE type LIKE '%500%' OR type LIKE '%000%'
      GROUP BY elev
    """)
    pnts = sql()
    elevations = ""
    for pnt in pnts:
        elevations += f"({pnt[0]}, '{pnt[1]}'::geometry),"
    return elevations

def handle_unlabeled_rings(lines, points):
    """Handle unlabeled rings."""
    LOGGER.info("Unlabeled rings")
    sql(f"""
      SELECT topring.id, topring.wkb_geometry FROM {lines} AS topring
      WHERE ST_IsClosed(topring.wkb_geometry) AND (
        topring.type LIKE '%CONTOURS%' OR topring.type LIKE '%00'
        )
        AND EXISTS (
          SELECT * FROM {points} AS peaks
          WHERE type = 'PEAK' AND
            ST_Intersects(ST_MakePolygon(topring.wkb_geometry), peaks.wkb_geometry)
        )
        AND NOT EXISTS (
          SELECT * FROM {lines} AS covers
          WHERE (covers.type LIKE '%00' OR covers.type LIKE '%CONTOURS%') AND
            topring.id <> covers.id AND
            CASE WHEN ST_IsClosed(topring.wkb_geometry) THEN
              ST_Covers(ST_MakePolygon(topring.wkb_geometry), covers.wkb_geometry)
            END
        )
    """)
    rings = sql()
    for ring in rings:
        label_rings(lines, ring)

def label_rings(lines, ring):
    """Label all unlabeled rings and check label consistency."""
    LOGGER.debug("ring %s", ring[0])
    sql(f"""
      SELECT id, type FROM {lines}
      WHERE (type LIKE '%CONTOURS%' OR type LIKE '%00') AND
        CASE WHEN ST_IsClosed(wkb_geometry) THEN
          ST_Covers(ST_MakePolygon(wkb_geometry), '{ring[1]}'::geometry)
        END
      ORDER BY ST_Distance(wkb_geometry, '{ring[1]}'::geometry) ASC
    """)
    rings = list(enumerate(sql()))
    for idx_r, type_r in rings:
        if "00" not in type_r[1]:
            continue
        LOGGER.debug("- found fixed %s", type_r)
        for idx_c, type_c in rings:
            elevation = int(type_r[1]) + 500*(idx_r - idx_c)
            LOGGER.debug("- - fix %s with %s", type_c, elevation)
            if f"{elevation}" != type_c[1] and "CONTOURS" not in type_c[1]:
                LOGGER.debug("- - - errorneous fix %s with %s", type_c, elevation)

            sql(f"""
              UPDATE {lines} SET type = '{elevation}' WHERE id = {type_c[0]} AND type LIKE '%CONTOURS%'
            """)

def execute(lines, points, polygons):
    """Top-level work-horse function. Connects lines, labels, etc."""
    # Initialize
    sql(f"""
      CREATE TEMP SEQUENCE IF NOT EXISTS serial START 400000;
      ALTER TABLE {lines} ALTER id SET NOT NULL;
      SELECT count(*) FROM {lines} WHERE type LIKE '%CONTOURS%'
    """)
    LOGGER.info("Identifying lines: %s", sql()[0][0])

    # Remove pathological lines
    LOGGER.info("Remove lines of length 3")
    sql(f"""
      DELETE FROM {lines}
      WHERE type LIKE '%CONTOURS%' AND ST_NumPoints(wkb_geometry) < 4 OR ST_Length(wkb_geometry) < {EPSL}
    """)

    # Special corrections
    LOGGER.info("Remove spurious lines")
    LOGGER.debug("duplicate at LM5") # delicate
    approx = 'POLYGON((-17.0025 45.7429, ' + \
        '-17.0023 45.7429, -17.0023 45.7426, ' + \
        '-17.0025 45.7426, -17.0025 45.7429))'
    sql(f"DELETE FROM {lines} WHERE ST_Intersects(wkb_geometry, '{approx}'::geometry)")

    # Remove self intersections
    LOGGER.info("Validate lines")
    sql(f"SELECT id, wkb_geometry FROM {lines} WHERE type LIKE '%CONTOURS%'")
    rings = sql()
    for ring in rings:
        make_valid(lines, [ring[1]], ring[0])

    # Match labels and lines
    LOGGER.info("Matching height label to lines")
    elevations = sort_elevation_pts(points)
    sql(f"""
      UPDATE {lines}
      SET type = t3.b FROM (
        WITH elev (idx, geom) AS (VALUES {elevations[:-1]})
        SELECT t1.id, t2.idx || '00' FROM {lines} AS t1 JOIN elev AS t2 ON TRUE
        WHERE ST_Distance(t2.geom, t1.wkb_geometry) < {EPSP}
        ORDER BY ST_Distance(t2.geom, t1.wkb_geometry)
      )
      AS t3 (a, b)
      WHERE id = t3.a AND type LIKE '%CONTOURS%'
    """)

    sql(f"SELECT count(*) FROM {lines} WHERE type LIKE '%CONTOURS%'")
    LOGGER.info("Remaining lines: %s", sql()[0][0])

    # Connect across small gaps
    LOGGER.info("Connect unlabeled and like-labeled lines")
    sql(f"""
      SELECT id, type FROM {lines}
      WHERE type LIKE '%00' AND NOT ST_IsClosed(wkb_geometry) ORDER BY id
    """)
    rings = sql()
    deleted = []
    for ring in rings:
        if ring[0] in deleted:
            continue
        LOGGER.debug("connect %s", ring[0])
        connect = shortest_connect(lines, ring[0], ring[1], EPSL)
        while len(connect) > 0:
            LOGGER.debug("- with %s", connect[0][0])
            make_valid(lines, connect[0][2:], ring[0])
            if ring[0] == connect[0][0]:
                break
            LOGGER.debug("- remove %s", connect[0][0])
            sql(f"DELETE FROM {lines} WHERE id = {connect[0][0]}")
            deleted.append(connect[0][0])
            connect = shortest_connect(lines, ring[0], ring[1], EPSL)

    # Closed non-labelled
    sql(f"SELECT count(*) FROM {lines} WHERE type LIKE '%CONTOURS%'")
    LOGGER.info("Remaining lines: %s", sql()[0][0])

    handle_unlabeled_rings(lines, points)

    # Convert to polygons
    LOGGER.info("Turn closed lines into polygons")
    sql(f"""
      INSERT INTO {polygons} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), 'elevation', type, ST_MakePolygon(wkb_geometry)
      FROM {lines}
      WHERE type LIKE '%00' AND ST_IsClosed(wkb_geometry)
      RETURNING type
    """)
    LOGGER.debug("Converted %s to polygons", len(sql()))

    # Rest
    sql(f"SELECT count(*) FROM {lines} WHERE type LIKE '%CONTOURS%'")
    LOGGER.info("Remaining lines: %s", sql()[0][0])

def main(inpre):
    """Main method."""

    points = f"{inpre}_points"
    lines = f"{inpre}_lines"
    polygons = f"{inpre}_polygons"

    execute(lines, points, polygons)

def tests(inpre):
    """Test collector."""
    points = f"{inpre}_points"
    lines = f"{inpre}_lines"
    polygons = f"{inpre}_polygons"

    # Initialize
    sql(f"CREATE TEMP SEQUENCE IF NOT EXISTS serial START 400000")

    simple_tests(lines, points, polygons)

def simple_tests(lines, points, polygons):
    """Simple tests. 1 segment line, 2 segment line."""

    # Priming test DB
    sql(f"DELETE FROM {lines}")
    sql(f"DELETE FROM {points}")
    sql(f"DELETE FROM {polygons}")

    sql(f"""
      INSERT INTO {points} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), '500', 'Forest - 500&apos;', 'POINT(1 0)'::geometry)
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'line-500', 'CONTOURS', 'LINESTRING(0.002 0, 2.002 0, 2 1, 0 1, 0.002 0)'::geometry)
    """)

    sql(f"""
      INSERT INTO {points} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), '1000', 'Alpine - 1000&apos;', 'POINT(1 2)'::geometry)
    """)
    sql(f"""
      INSERT INTO {points} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), '1000', 'Woodland - 1000&apos;', 'POINT(0 2.5)'::geometry)
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'line-1000', 'CONTOURS', 'LINESTRING(0.002 2, 1.002 2, 2.002 2, 2 3, 0 3)'::geometry)
    """)
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      VALUES (nextval('serial'), 'line-1000', 'CONTOURS', 'LINESTRING(0 3, 0.002 2.6, 0.002 2.4, 0.002 2)'::geometry)
    """)

    execute(lines, points, polygons)

    sel = f"SELECT count(*) FROM {polygons} WHERE "
    num_tests = 0

    sql(f"{sel} type = '500'")
    num_tests += 1
    assert sql()[0][0] == 1

    sql(f"{sel} type = '1000'")
    num_tests += 1
    assert sql()[0][0] == 1
    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")
