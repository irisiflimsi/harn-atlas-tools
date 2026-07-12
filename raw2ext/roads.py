#!/usr/bin/python
"""
Connects all roads with either endpoints or the next closest road
if within reasonable distance.
"""
import logging
from raw2ext import sql

LOGGER = logging.getLogger(__name__)

EPSG = 0.005 # gap to bridge

def connect_network(lines, pt_lines, index):
    """Modify network to connect two adjacent lines."""
    for pt_line in pt_lines:
        LOGGER.debug("- start/end %s on %s", pt_line[1], pt_line[0])
        sql(f"""
          UPDATE {lines}
          SET wkb_geometry = ST_Snap(wkb_geometry, '{pt_line[2]}'::geometry, {EPSG*1.01})
          WHERE id = {pt_line[0]}
        """)
        sql(f"""
          UPDATE {lines}
          SET wkb_geometry = ST_SetPoint(wkb_geometry, {index}, '{pt_line[2]}'::geometry)
          WHERE id = {pt_line[1]}
        """)

def main(inpre):
    """Main method."""

    lines = f"{inpre}_lines"
    points = f"{inpre}_points"

    # Initialize
    sql(f"""
      CREATE TEMP SEQUENCE IF NOT EXISTS serial START 200000;
      ALTER TABLE {lines} ALTER id SET NOT NULL;
      DELETE FROM {lines} WHERE type = 'ROUTE';
      SELECT count(*) FROM {lines} WHERE type LIKE '%ROADS%'
    """)
    LOGGER.info("Identifying lines: %s", sql()[0][0])

    # '%00%','%Abbey%','%BRIDGE%','%Battle Site%','%Castle%',
    # '%Chapter House%','%City%','%Ferry%','%Ford%','%Fort%','%Gargun%',
    # '%Keep%','%Manor%','%Mine%','%PEAK%','%Quarry%','%ROAD%' <- Tollhouse
    # '%Rapids%','%Ruin%','%SEA%','%SWAMP%','%Salt%','%Special %',
    # '%special %','%Swamp%','%TOWNS%','%Tribal%','%Waterfall%'
    sql_locs = "type LIKE '%Abbey%' OR \
          type LIKE '%BRIDGE%' OR \
          type LIKE '%Chapter House%' OR \
          type LIKE '%City' OR \
          type LIKE '%Ferry%' OR \
          type LIKE '%Ford%' OR \
          type LIKE '%Fort%' OR \
          type LIKE '%Gargun%' OR \
          type LIKE '%Keep%' OR \
          type LIKE '%Manor%' OR \
          type LIKE '%Mine%' OR \
          type LIKE '%Quarry%' OR \
          type LIKE '%ROAD%' OR \
          type LIKE '%Salt%' OR \
          type LIKE '%Special%' OR \
          type LIKE '%special%' OR \
          type LIKE '%TOWNS%' OR \
          type LIKE '%Tribal%' OR \
          type LIKE '%Castle%'"

    # Get all locations
    sql(f"SELECT ST_Union(wkb_geometry) FROM {points} WHERE {sql_locs}")
    pts = sql()[0][0]

    # Shift all roads onto locations
    sql(f"""
      SELECT t1.id, array_agg(t2.id), t1.wkb_geometry
      FROM {points} AS t1 INNER JOIN LATERAL (
        SELECT id, wkb_geometry FROM {lines}
        WHERE type LIKE '%ROADS%' AND
          ST_Distance(wkb_geometry, t1.wkb_geometry) < {EPSG} AND
          ST_Distance(wkb_geometry, t1.wkb_geometry) <> 0
      )
      AS t2 (id, geo) ON TRUE
      WHERE {sql_locs}
      GROUP BY t1.id
    """)
    pt_lines = sql()
    LOGGER.info("Shift %s roads onto locations", len(pt_lines))
    for pt_line in pt_lines:
        LOGGER.debug("- shift onto %s", pt_line[0])
        for pt_i in pt_line[1]:
            sql(f"""
              UPDATE {lines}
              SET wkb_geometry = ST_Snap(wkb_geometry, '{pt_line[2]}'::geometry, {EPSG*1.01})
              WHERE id = {pt_i}
            """)

    # Shift all road starts/ends
    sql(f"""
      SELECT t1.id, t2.id, ST_ClosestPoint(t1.wkb_geometry, ST_StartPoint(t2.geo))
      FROM {lines} AS t1 INNER JOIN LATERAL (
        SELECT t3.id, t3.wkb_geometry FROM {lines} AS t3
        WHERE t3.id <> t1.id AND t3.type LIKE '%ROADS%' AND
          ST_Distance(ST_StartPoint(t3.wkb_geometry), t1.wkb_geometry) < {EPSG} AND
          ST_Distance(ST_StartPoint(t3.wkb_geometry), '{pts}'::geometry) > {EPSG/2}
      )
      AS t2 (id, geo) ON TRUE
      WHERE t1.type LIKE '%ROADS%'
    """)
    pt_lines = sql()
    LOGGER.info("Shift %s road-starts onto roads", len(pt_lines))
    # Make adjacent lines include new start points and ending lines end in new start points
    connect_network(lines, pt_lines, 0)

    sql(f"""
      SELECT t1.id, t2.id, ST_ClosestPoint(t1.wkb_geometry, ST_EndPoint(t2.geo))
      FROM {lines} AS t1 INNER JOIN LATERAL (
        SELECT t3.id, t3.wkb_geometry FROM {lines} AS t3
        WHERE t3.id <> t1.id AND t3.type LIKE '%ROADS%' AND
          ST_Distance(ST_EndPoint(t3.wkb_geometry), t1.wkb_geometry) < {EPSG} AND
          ST_Distance(ST_EndPoint(t3.wkb_geometry), '{pts}'::geometry) > {EPSG/2}
      )
      AS t2 (id, geo) ON TRUE
      WHERE t1.type LIKE '%ROADS%'""")
    pt_lines = sql()
    LOGGER.info("Shift %s road-end onto roads", len(pt_lines))
    # Make adjacent line include new end point and ending line end in new end point
    connect_network(lines, pt_lines, -1)

    LOGGER.info("Remove some artifacts")
    sql(f"""
      SELECT id, ST_NPoints(wkb_geometry) FROM {lines}
      WHERE type LIKE '%ROADS%' AND
        ST_Distance(ST_StartPoint(wkb_geometry), '{pts}'::geometry) < {EPSG} AND
        ST_Distance(ST_StartPoint(wkb_geometry), '{pts}'::geometry) <> 0
    """)
    pt_lines = sql()
    for pt_line in pt_lines:
        if pt_line[1] > 2:
            sql(f"""
              UPDATE {lines}
              SET wkb_geometry = ST_RemovePoint(wkb_geometry, 0)
              WHERE id = {pt_line[0]}
            """)
        else:
            sql(f"DELETE FROM {lines} WHERE id = {pt_line[0]}")

    sql(f"""
      SELECT id, ST_NPoints(wkb_geometry) FROM {lines}
      WHERE type LIKE '%ROADS%' AND
        ST_Distance(ST_EndPoint(wkb_geometry), '{pts}'::geometry) < {EPSG} AND
        ST_Distance(ST_EndPoint(wkb_geometry), '{pts}'::geometry) <> 0
    """)
    pt_lines = sql()
    for pt_line in pt_lines:
        if pt_line[1] > 2:
            sql(f"""
              UPDATE {lines}
              SET wkb_geometry = ST_RemovePoint(wkb_geometry, {pt_line[1] - 1})
              WHERE id = {pt_line[0]}
            """)
        else:
            sql(f"DELETE FROM {lines} WHERE id = {pt_line[0]}")

    LOGGER.info("Make all trails")
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'Trail', t1.geo FROM (
        SELECT (ST_Dump(ST_LineMerge(ST_Union(wkb_geometry)))).geom FROM {lines}
        WHERE type LIKE '%ROADS%' AND style LIKE '%dasharray: 1 1%'
      )
      AS t1 (geo)
    """)
    LOGGER.info("Make all unpaved roads")
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'Unpaved', t1.geo FROM (
        SELECT (ST_Dump(ST_LineMerge(ST_Union(wkb_geometry)))).geom FROM {lines}
        WHERE type LIKE '%ROADS%' AND style LIKE '%dasharray: 2 1%'
      )
      AS t1 (geo)
    """)
    LOGGER.info("Make all paved roads")
    sql(f"""
      INSERT INTO {lines} (id, name, type, wkb_geometry)
      SELECT nextval('serial'), '-', 'Paved', t1.geo FROM (
        SELECT (ST_Dump(ST_LineMerge(ST_Union(wkb_geometry)))).geom FROM {lines}
        WHERE type LIKE '%ROADS%' AND style NOT LIKE '%dasharray:%'
      )
      AS t1 (geo)
    """)
