#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Detect all local map stuff.
"""
import re
import logging
from raw2ext import sql, shortest_connect, make_valid

LOGGER = logging.getLogger(__name__)

EPS = 3e-6 # Potential inaccuracy in SVG
EPSE = 3e-4 # Distance to elevation label

WHITE = "fill:#FFFFFF;"
STROKE = "stroke:#000000;"

def execute(polygons, lines, points):
    """Actual main method."""

    artifacts(polygons, lines, points)
    contours(polygons, lines, points)
    towers(polygons)
    houses(polygons, lines, points)
    numbers(polygons, points)

def artifacts(polygons, lines, points):
    """Remove artifacts from export."""
    # Remove
    sql(f"""
      DELETE FROM {polygons}
      WHERE type LIKE '%fill:-;stroke:-;%' OR type LIKE 'Trees_Large_2.tif_%' OR
        type LIKE '%fill:#FFFFFF;stroke:#FFFFFF;%';
      DELETE FROM {points} WHERE type = 'Flow';
    """)
    # Add (Cherafir)
    sql(f"""
      INSERT INTO {lines} (id, type, wkb_geometry) VALUES (
        2001, '10', 'LINESTRING(
          -15.8352840 40.3930313,
          -15.8351529 40.3926221,
          -15.8349182 40.3923880
        )'::geometry
      ),
      (
        2002, '10', 'LINESTRING(
          -15.8325925 40.3950085,
          -15.8325520 40.3948112,
          -15.8323945 40.3946609,
          -15.8323092 40.3944631
        )'::geometry
      ),
      (
        2003, '10', 'LINESTRING(
          -15.8325906 40.3933906,
          -15.8327751 40.3931144,
          -15.8331093 40.3926326,
          -15.8332906 40.3925558,
          -15.8335891 40.3922019,
          -15.8340603 40.3921267
        )'::geometry
      );
      INSERT INTO {points} (id, type, wkb_geometry) VALUES (
        2004, '10', 'POINT(-15.8324559 40.3935786)'::geometry
      );
      UPDATE {points} SET type = regexp_replace(type, 'Terrain_Features (.+)', '\\1')
        WHERE type LIKE 'Terrain_Features %';
      UPDATE {points} SET type = regexp_replace(type, '([A-Z])_-_.* ([0-9a-z]+)', '\\1 \\2')
        WHERE type LIKE '%\\_-\\_%';
    """)

def contours(polygons, lines, points):
    """Extract contours."""
    sql(f"""
      SELECT id, wkb_geometry, ST_Xmin(wkb_geometry), ST_Ymin(wkb_geometry),
        ST_Xmax(wkb_geometry), ST_Ymax(wkb_geometry)
      FROM {polygons} WHERE ST_Area(wkb_geometry) > 1e-6;
    """)
    frame = sql()[0]
    sql(f"""
      UPDATE {polygons} SET type = 'frame' WHERE id = {frame[0]}
    """)
    LOGGER.info("frame id %s", frame[0])
    sql(f"""
      UPDATE {lines} SET type = t2type FROM (
        SELECT t1.id AS t1id, t2type FROM {lines} as t1, LATERAL (
          SELECT t2.type AS t2type FROM {points} AS t2
          WHERE t2.type IN ('10', '20', '30', '40') AND
            ST_Distance(t1.wkb_geometry, t2.wkb_geometry) < {EPSE}
            AND ST_Intersects(t2.wkb_geometry, ST_Buffer('{frame[1]}'::geometry, -{EPS}))
          ORDER BY ST_Distance(t1.wkb_geometry, t2.wkb_geometry)
          LIMIT 1
        )
        WHERE t1.type LIKE '%stroke:#962F34%'
      )
      WHERE id = t1id
      RETURNING id, type
    """)
    LOGGER.debug("positive elevations: %s", sql())
    sql(f"""
      UPDATE {lines} SET type = '0',
        wkb_geometry = ST_Intersection(wkb_geometry, '{frame[1]}'::geometry)
      WHERE type LIKE '%water%'
      RETURNING id
    """)
    LOGGER.debug("waterline: %s", sql())

    # Connect across small gaps
    LOGGER.info("Connect disconnected lines")
    sql(f"SELECT id, type FROM {lines} WHERE type LIKE '%0'")
    rings = sql()
    deleted = []
    for ring in rings:
        if ring[0] in deleted:
            continue
        LOGGER.debug("connect %s", ring[0])
        connect = shortest_connect(f"{lines}", ring[0], ring[1], EPSE)
        while len(connect) > 0:
            LOGGER.debug("- with %s", connect[0][0])
            make_valid(lines, connect[0][2:], ring[0])
            if ring[0] == connect[0][0]:
                break
            LOGGER.debug("- remove %s", connect[0][0])
            sql(f"DELETE FROM {lines} WHERE id = {connect[0][0]}")
            deleted.append(connect[0][0])
            connect = shortest_connect(f"{lines}", ring[0], ring[1], 2*EPSE)

    sql(f"""
      UPDATE {lines} SET wkb_geometry = ST_Intersection(wkb_geometry, '{frame[1]}'::geometry)
      WHERE type LIKE '%0'
    """)
    sql(f"""
      UPDATE {lines} AS t1 SET wkb_geometry = geo6 FROM (
        SELECT id AS id6, (ST_Dump(ST_LineMerge(geo5))).geom AS geo6 FROM (
          SELECT id, wkb_geometry,
            ST_Union(
              ST_Translate(
                ST_Scale(ST_Translate(geo3, 0, {-frame[5]}), 1, -1), 0, {frame[5]}
              ),
              geo4
            )
            AS geo5
          FROM (
            SELECT id, wkb_geometry, geo3,
              ST_Union(
                ST_Translate(
                  ST_Scale(ST_Translate(geo3, 0, {-frame[3]}), 1, -1), 0, {frame[3]}
                ),
                geo3
              )
              AS geo4
            FROM (
              SELECT id, wkb_geometry,
                ST_Union(
                  ST_Translate(
                    ST_Scale(ST_Translate(wkb_geometry, {-frame[4]}, 0), -1, 1), {frame[4]}, 0
                  ),
                  geo2
                )
                AS geo3
              FROM (
                SELECT id, wkb_geometry,
                  ST_Union(
                    ST_Translate(
                      ST_Scale(ST_Translate(geo1, {-frame[2]}, 0), -1, 1), {frame[2]}, 0
                    ),
                    geo1
                  )
                  AS geo2
                FROM (
                  SELECT id, wkb_geometry, ST_Intersection(wkb_geometry, '{frame[1]}'::geometry)
                  AS geo1
                  FROM {lines}
                  WHERE type LIKE '%0' AND
                    ST_Intersects(wkb_geometry, '{frame[1]}'::geometry) AND
                    NOT ST_IsClosed(wkb_geometry)
                )
              )
            )
          )
        )
        GROUP BY id6, geo6
      )
      WHERE t1.id = id6 AND ST_Intersects(geo6, t1.wkb_geometry)
    """)
    sql(f"""
      INSERT INTO {polygons} (id, type, wkb_geometry)
      SELECT id, type, ST_MakePolygon(wkb_geometry) FROM {lines}
      WHERE type LIKE '%0' AND ST_IsClosed(wkb_geometry);
      DELETE FROM {lines}
      WHERE type LIKE '%0' AND ST_IsClosed(wkb_geometry);
      SELECT id FROM {lines} WHERE type LIKE '%0'
    """)
    LOGGER.debug("horizontal or vertical contours %s", sql())
    sql(f"""
      INSERT INTO {polygons} (id, type, wkb_geometry)
      SELECT id + random(), type, (
          ST_Dump(
            ST_CollectionExtract(
              ST_Polygonize(
                ST_Union(
                  wkb_geometry,
                  ST_Translate(
                    ST_Scale(
                      ST_Translate(
                        ST_Boundary('{frame[1]}'::geometry),
                        {-(frame[2] + frame[4])/2}, {-(frame[3] + frame[5])/2}
                      ),
                      3, 3
                    ),
                    {(frame[2] + frame[4])/2}, {(frame[3] + frame[5])/2}
                  )
                )
              ),
              3
            )
          )
        ).geom
      FROM {lines} WHERE type LIKE '%0' GROUP BY id
      RETURNING id, type, ST_AsText(wkb_geometry);
      DELETE FROM {lines} WHERE type LIKE '%0'
    """)

def towers(polygons):
    """Extract towers."""
    LOGGER.info("detect towers")
    sql(f"""
      UPDATE {polygons} SET type = 'Tower' WHERE type LIKE '%fill:-;{STROKE}%'
    """)

def houses(polygons, lines, points):
    """Extract houses."""
    LOGGER.info("convert circles to blocks")
    add_points(polygons, points)

    LOGGER.info("convert polygons and filled lines to blocks")
    sql(f"""
      SELECT id, 'polygon', type, wkb_geometry, ST_NPoints(wkb_geometry) FROM {polygons}
      WHERE type LIKE '%{WHITE}%'
      UNION
      SELECT id, 'line', type, wkb_geometry, ST_NPoints(wkb_geometry) FROM {lines}
      WHERE type LIKE '%{WHITE}%'
      ORDER BY id
    """)
    rows = sql()
    for row in rows:
        if row[1] == 'polygon' and f"{WHITE}{STROKE}" in row[2]:
            LOGGER.debug("found single %s", row[0])
            subtract_new_from_previous(polygons, row)
        elif row[1] == 'polygon' and f"{WHITE}stroke:-;" in row[2]:
            LOGGER.debug("found joiner %s", row[0])
            sql(f"""
              SELECT t1.id, t1.type
              FROM {polygons} AS t1, {polygons} AS t2
              WHERE t2.id = {row[0]} AND ST_Intersects(t1.wkb_geometry, t2.wkb_geometry) AND
                t1.id < t2.id AND t1.type LIKE '%floor%'
            """)
            floors = sql()
            if len(floors) > 0:
                ifloors = [floor[0] for floor in floors]
                sfloors = "','".join([str(floor[0]) for floor in floors])
                LOGGER.debug("- intersects %s", ifloors)
                min_floor = min(ifloors)
                sql(f"""
                  UPDATE {polygons} AS t1 SET wkb_geometry = (
                    SELECT geo FROM (
                      SELECT (ST_Dump(ST_Union(ST_MakeValid(t2.wkb_geometry)))).geom AS geo
                      FROM {polygons} AS t2
                      WHERE t2.id IN ('{sfloors}') OR t2.id = {row[0]}
                    )
                    ORDER BY ST_Area(geo) DESC LIMIT 1
                  )
                  WHERE id = '{min_floor}';
                  DELETE FROM {polygons} WHERE id IN ('{sfloors}','{row[0]}') AND
                    id <> '{min_floor}'
                """)
        elif row[1] == 'polygon':
            LOGGER.warning("cannot allocate %s", row)
        elif row[1] == 'line' and f"{WHITE}{STROKE}" in row[2] and row[4] > 3:
            LOGGER.debug("found partial %s", row[0])
            sql(f"""
              INSERT INTO {polygons} (id, type, wkb_geometry)
              SELECT {row[0]} + random(), '{WHITE}{STROKE}',
                ST_MakePolygon(ST_AddPoint(wkb_geometry, ST_StartPoint(wkb_geometry)))
              FROM {lines} WHERE id = {row[0]}
              RETURNING id, 'polygon', type, ST_AsText(wkb_geometry), ST_NPoints(wkb_geometry);
            """)
            new_row = sql()[0]
            sql(f"""
              SELECT ST_AsText(ST_Difference(ST_Boundary(t1.wkb_geometry), t2.wkb_geometry))
              FROM {polygons} AS t1, {lines} AS t2
              WHERE t1.id = {new_row[0]} AND t2.id = {row[0]}
            """)
            line = sql()[0] # The white line
            sql(f"""
              DELETE FROM {lines} WHERE id = {row[0]}
            """)
            if 'EMPTY' not in line[0]:
                sql(f"""
                  SELECT id, (
                    ST_Dump(
                      ST_CollectionExtract(ST_Union(wkb_geometry, '{new_row[3]}'::geometry), 3)
                    )
                  ).geom
                  FROM {polygons}
                  WHERE id < {row[0]} AND type LIKE '%floor%' AND
                    ST_Intersects(wkb_geometry, '{line[0]}'::geometry) AND 
                    ST_Length(ST_Intersection(wkb_geometry, '{line[0]}'::geometry)) > {2*EPS}
                """)
                for merge in sql():
                    sql(f"""
                      UPDATE {polygons} SET wkb_geometry = '{merge[1]}'::geometry
                      WHERE id = {new_row[0]};
                      DELETE FROM {polygons} WHERE id = {merge[0]}
                    """)
            subtract_new_from_previous(polygons, new_row)
    LOGGER.info("convert non-filled lines to blocks")
    sql(f"""
      SELECT id, wkb_geometry FROM {polygons} WHERE type = 'Ground floor' ORDER BY id
    """)
    for row in sql():
        LOGGER.debug("check %s", row[0])
        sql(f"""
          SELECT ST_Union(geo) FROM (
            SELECT ST_LineExtend(
              (
                ST_Dump(
                  ST_CollectionExtract(
                    ST_Intersection(
                      ST_MakeValid(ST_Union(ST_MakeValid(wkb_geometry))),
                      ST_MakeValid('{row[1]}'::geometry)
                    ),
                    2
                  )
                )
              ).geom,
              {EPS}, {EPS}) AS geo
            FROM {lines} WHERE type LIKE '%fill:none;{STROKE}%'
          )
        """)
        grid = sql() # The wall grid in building
        if grid[0][0] is not None:
            sql(f"""
              INSERT INTO {polygons} (id, type, wkb_geometry)
              SELECT {row[0]} + random(), 'Ground floor', geo FROM (
                SELECT (
                  ST_Dump(
                    ST_CollectionExtract(
                      ST_Polygonize(
                        ST_Union(ST_Boundary('{row[1]}'::geometry), '{grid[0][0]}'::geometry)),
                        3
                      )
                    )
                  ).geom
                  AS geo
              )
              RETURNING id
            """)
            LOGGER.debug("- extracted %s", sql())
            sql(f"DELETE FROM {polygons} WHERE id = {row[0]}")

def numbers(polygons, points):
    """Attach numbers to their houses."""
    sql(f"""
      SELECT t1.id, t2.id, t1.type FROM {points} AS t1, {polygons} AS t2
      WHERE t1.type SIMILAR TO '[A-Z] [0-9]+' AND t2.type = 'Ground floor' AND
        ST_Covers(t2.wkb_geometry, t1.wkb_geometry)
    """)
    for row in sql():
        sql(f"""
          UPDATE {polygons} SET type = type || ' (' || '{row[2]}' || ')'
          WHERE id = {row[1]};
          DELETE FROM {points} WHERE id = {row[0]}
        """)
    sql(f"""
      DELETE FROM {points} WHERE type LIKE 'Ground floor (%'
    """)

def subtract_new_from_previous(polygons, row):
    """Subtract a new from existing bordered polygons."""
    sql(f"""
      SELECT t2.type 
      FROM {polygons} AS t1, {polygons} AS t2
      WHERE t1.id = {row[0]} AND ST_Contains(t2.wkb_geometry, t1.wkb_geometry) AND
      t1.id > t2.id
    """)
    floors = sql()
    floor = 'Ground floor'
    if 'Ground floor' in [floor[0] for floor in floors]:
        floor = '1st floor'
    sql(f"""
      UPDATE {polygons} SET type = '{floor}' WHERE id = {row[0]}
    """)
    sql(f"""
      INSERT INTO {polygons} (id, type, wkb_geometry)
      SELECT {row[0]} + random(), t1.type, (
        ST_Dump(
          ST_CollectionExtract(
            ST_Difference(ST_MakeValid(t1.wkb_geometry), ST_MakeValid(t2.wkb_geometry)), 3
          )
        )
      ).geom
      FROM {polygons} AS t2, {polygons} AS t1
      WHERE t2.id = {row[0]} AND t1.type LIKE '%floor%' AND t1.id < {row[0]} AND ST_Intersects(t1.wkb_geometry, t2.wkb_geometry);
      DELETE FROM {polygons} AS t1 USING {polygons} AS t2
      WHERE t2.id = {row[0]} AND t1.type LIKE '%floor%' AND t1.id < {row[0]} AND ST_Intersects(t1.wkb_geometry, t2.wkb_geometry)
    """)

def get_sql_walls(width):
    """Get sql for lines representing walls"""
    return f"""
      type LIKE '%fill:none;{STROKE}%' AND
        {width * 0.8} <= CAST(
          regexp_replace(type, '.*stroke-width:([0-9.]+).*', '\\1') AS float
        ) AND
        {width * 1.2} >= CAST(
          regexp_replace(type, '.*stroke-width:([0-9.]+).*', '\\1') AS float
        ) AND
        ST_NPoints(wkb_geometry) <= 75 -- heuristic from Cherafir --
    """

def get_wall_width(polygons):
    """Get house wall width in svg units"""
    LOGGER.debug("- get house wall width")
    sql(f"""
      SELECT CAST(
        regexp_replace(type, '.*stroke-width:([0-9.]+).*', '\\1') AS float
      )
      FROM {polygons}
      WHERE type LIKE '%{WHITE}{STROKE}%'
      LIMIT 1
    """)
    width = float(sql()[0][0])
    LOGGER.debug("- - width: %f", width)
    return width

def add_points(polygons, points):
    """Transfer circles."""
    sql(f"""
      SELECT DISTINCT id, wkb_geometry, type
      FROM {points}
      WHERE type LIKE '%{WHITE}{STROKE}%'
    """)
    circles = sql()
    LOGGER.debug("- circles to transfer: %s", len(circles))
    for circle in circles:
        LOGGER.debug("- transfer %s", circle[0])
        match = re.match(r"radius:([0-9-e\.]+)", circle[2])
        radius = float(match.group(1))
        sql(f"""
          INSERT INTO {polygons} (id, wkb_geometry, type)
          VALUES ('{circle[0]}', ST_Buffer('{circle[1]}'::geometry, {radius}), '{circle[2]}');
          DELETE FROM {points} WHERE id = {circle[0]} 
        """)

def main(inpre):
    """Main method."""

    # Initialize
    sql(f"CREATE TEMP SEQUENCE IF NOT EXISTS serial START 500000")
    sql(f"ALTER TABLE {inpre}_polygons ALTER COLUMN id TYPE float USING id::float")
    lines = f"{inpre}_lines"
    polygons = f"{inpre}_polygons"
    points = f"{inpre}_points"
    execute(polygons, lines, points)
