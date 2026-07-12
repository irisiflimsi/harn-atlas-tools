#!/usr/bin/python
"""
Creates all vegetation (including shoal/reef).
"""
import logging
from raw2ext import sql

LOGGER = logging.getLogger(__name__)

EPSG = 0.00025 # grow to cover draw glitches
EPSI = 0.01 # grow swamp
EPSD = 0.0125 # shrink swamp

def geo_array(rows):
    """Create a postgis geometry array."""
    return "'" + "'::geometry, '".join([row[0] for row in rows]) + "'::geometry"

def make_swamp(lines, polygons):
    """Make Swamp out of various pieces."""

    # Areas as lines
    sql(f"""
      SELECT topring.id, ST_MakeValid(ST_MakePolygon(topring.wkb_geometry))
      FROM {lines} AS topring
      WHERE ST_IsClosed(topring.wkb_geometry) AND
        topring.type LIKE '%SWAMP%' AND
        NOT EXISTS (
          SELECT * FROM {lines} AS covers
          WHERE covers.type LIKE '%SWAMP%' AND
            topring.id <> covers.id AND
            CASE WHEN ST_IsClosed(covers.wkb_geometry) THEN
              ST_Covers(ST_MakePolygon(covers.wkb_geometry), topring.wkb_geometry)
            END
        )
    """)
    ret = []
    for poly in sql():
        sql(f"""
          SELECT ST_Union(ST_MakeValid(ST_MakePolygon(wkb_geometry)))
          FROM {lines}
          WHERE type LIKE '%SWAMP%' AND ST_NPoints(wkb_geometry) > 3 AND
            {poly[0]} <> id AND
            CASE WHEN ST_IsClosed(wkb_geometry) THEN
              ST_Covers('{poly[1]}'::geometry, ST_MakePolygon(wkb_geometry))
            END
        """)
        holes = sql()[0]
        LOGGER.debug("- swamp poly %s", poly[0])
        if holes[0] is not None:
            LOGGER.debug("- - with holes")
            sql(f"SELECT ST_Difference('{poly[1]}'::geometry, '{holes[0]}'::geometry)")
            ret.append([sql()[0][0]])
        else:
            ret.append([poly[1]])

    # Symbols on polys
    sql(f"""
      SELECT ST_Buffer(
        ST_Buffer(ST_Buffer(ST_Union(ST_MakeValid(wkb_geometry)), {EPSI}), -{EPSD}), {EPSD}
      )
      FROM {polygons}
      WHERE type LIKE '%SWAMP%'
    """)
    ret.append([sql()[0][0]])

    # Symbols on lines
    sql(f"""
      SELECT ST_Buffer(
        ST_Buffer(ST_Buffer(ST_Union(wkb_geometry), {EPSI}), -{EPSD}), {EPSD}
      )
      FROM {lines}
      WHERE NOT ST_IsClosed(wkb_geometry) AND type LIKE '%SWAMP%'
    """)
    ret.append([sql()[0][0]])
    return ret

def main(inpre):
    """Main method."""

    lines = f"{inpre}_lines"
    polygons = f"{inpre}_polygons"

    # Initialize
    types = [
        "WOODLAND", # default
        "CROPLAND",
        "HEATH",
        "SWAMP",
        "FOREST",
        "NEEDLELEAF",
        "ALPINE",
        "SNOW/ICE",
        "SHOAL/REEF"
    ]

    sql_area = "type LIKE '%" + "%' OR type LIKE '%".join(types) + "%'"
    # Initialize
    sql(f"""
      CREATE TEMP SEQUENCE IF NOT EXISTS serial START 300000;
      ALTER TABLE {polygons} ALTER id SET NOT NULL;
      SELECT count(*) FROM {lines} WHERE {sql_area}
    """)
    LOGGER.info("Identifying areas: %s", sql()[0][0])
    redux = {}
    raw = {}
    for typ in types:
        LOGGER.info("Set up %s", typ)
        if typ == "WOODLAND":
            sql(f"""
              SELECT ST_MakePolygon(wkb_geometry)
              FROM {lines}
              WHERE type = '0' AND ST_NPoints(wkb_geometry) > 3
            """)
            rows = list(sql())
            land = geo_array(rows)
            sql(f"""SELECT ST_Union(ARRAY[{land}])""")
            land_sql = sql()[0][0]
        elif typ == "SWAMP":
            rows = make_swamp(lines, polygons)
        else:
            sql(f"""
              SELECT ST_Buffer(
                ST_MakePolygon(ST_AddPoint(wkb_geometry, ST_StartPoint(wkb_geometry))),
                {EPSG}, 2
              )
              FROM {lines}
              WHERE type LIKE '%{typ}%' AND ST_NPoints(wkb_geometry) > 3
            """)
            rows = list(sql())
        raw[typ] = geo_array(rows)
        LOGGER.info("Found %s", len(rows))

    for i, ty_i in enumerate(types):
        LOGGER.info("Normalize %s", ty_i)
        redux[ty_i] = raw[ty_i]
        for j in range(i + 1, len(types) - 1):
            LOGGER.debug("- reduce %s in %s", ty_i, types[j])
            sql(f"SELECT ST_Union(ARRAY[{raw[types[j]]}])")
            sql(f"""
              SELECT ST_Difference(
                ST_Union(ARRAY[{redux[ty_i]}]), ST_Union(ARRAY[{raw[types[j]]}])
              )
            """)
            redux[ty_i] = geo_array(list(sql()))

        sql(f"""
          WITH ret AS (
            INSERT INTO {polygons} (id, name, type, wkb_geometry)
            SELECT nextval('serial'), '-', 'VEGTMP/{ty_i}', t1.geo FROM (
              SELECT (ST_Dump(ST_Union(ARRAY[{redux[ty_i]}]))).geom
            )
            AS t1 (geo)
            WHERE ST_GeometryType(t1.geo) = 'ST_Polygon' RETURNING id
          )
          SELECT * FROM ret
        """)
        LOGGER.debug("- normalized %s", len(sql()))

    LOGGER.info("Restrict real vegetation to land")
    sql(f"""
      INSERT INTO {polygons} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'VEG/' || t1.typ, t1.geo FROM (
        SELECT (ST_Dump(ST_Intersection(wkb_geometry, '{land_sql}'::geometry))).geom, substring(type, 8)
        FROM {polygons}
        WHERE type LIKE '%VEGTMP/%' AND type NOT LIKE '%SHOAL%'
      )
      AS t1 (geo, typ)
    """)
    LOGGER.info("Restrict shoal/reef to off land")
    sql(f"""
      INSERT INTO {polygons} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'VEG/' || t1.typ, t1.geo FROM (
        SELECT (ST_Dump(ST_Difference(wkb_geometry, '{land_sql}'::geometry))).geom, substring(type, 8)
        FROM {polygons}
        WHERE type LIKE '%VEGTMP/%' AND type LIKE '%SHOAL%'
      )
      AS t1 (geo, typ)
    """)
    sql(f"DELETE FROM {polygons} WHERE type LIKE '%VEGTMP/%'")
