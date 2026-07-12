#!/usr/bin/python
"""
Create peak names and height.
"""
import logging
from raw2ext import sql

LOGGER = logging.getLogger(__name__)

# Distance of name from (intended) POI
EPS = 0.04
# Distance for duplicates
EPSD = 0.001

def obtain_names(points, lines):
    """Associate names to peaks"""

    # Remove pure labels
    LOGGER.info("Remove pure labels")
    sql(f"""
      DELETE FROM {points}
      WHERE type LIKE 'Alpine %' OR
        type LIKE 'Needleleaf %' OR
        type LIKE 'Woodlands %' OR
        type LIKE 'Heath %' OR
        type LIKE 'Snow/Ice%' OR
        type LIKE 'Swamp%' OR
        type LIKE 'Forest %' OR
        type LIKE 'Cropland %' OR
        type LIKE '/WOODLAND/%' OR
        type LIKE '/SWAMPS/%' OR
        type LIKE '/SHOAL/%' OR
        type LIKE '/ROADS/%' OR
        type LIKE '/HEATH/%' OR
        type LIKE '/FOREST/%' OR
        type LIKE '/CROPLAND/%' OR
        type LIKE '/BOUNDARIES%'
    """)

    LOGGER.info("Label peaks")
    sql(f"""
      SELECT t1id, substring(t2name for 1) || lower(substring(rtrim(t2name,'0123456789') from 2)),
        ltrim(t2name,'A''BCDEFGHIJKLMNOPQRSTUVWXYZ')
      FROM (
        SELECT t1.id AS t1id, substring(t2.name from 10) AS t2name, dist FROM {points} AS t1,
        LATERAL (
          SELECT t3.name AS name, ST_Distance(t1.wkb_geometry, t3.wkb_geometry) AS dist
          FROM {points} AS t3
          WHERE regexp_like(t3.name, 'PeakName/') AND
            NOT (t3.name LIKE '%000' OR t3.name LIKE '%500')
          ORDER BY ST_Distance(t1.wkb_geometry, t3.wkb_geometry)
          LIMIT 1
        )
        AS t2
        WHERE t1.type = 'PEAK' AND dist < {EPS} ORDER BY dist DESC
      )
    """)
    for row in sql():
        row1 = row[1].replace("'", "''")
        sql(f"UPDATE {points} SET name = '{row1}', svgid = '{row[2]}' WHERE id = {row[0]}")

    LOGGER.info("Label lakes")
    sql(f"""
      SELECT t1id, substring(t2name for 1) || lower(substring(rtrim(t2name,'0123456789') from 2)),
        ltrim(t2name,'A''BCDEFGHIJKLMNOPQRSTUVWXYZ')
      FROM (
        SELECT t1.id AS t1id, substring(t2.name from 9) AS t2name, dist FROM {lines} AS t1,
        LATERAL (
          SELECT t3.name AS name, ST_Distance(t1.wkb_geometry, t3.wkb_geometry) AS dist
          FROM {points} AS t3
          WHERE regexp_like(t3.name, 'AnyName/Lake')
          ORDER BY ST_Distance(t1.wkb_geometry, t3.wkb_geometry)
          LIMIT 1
        )
        AS t2
        WHERE t1.style LIKE '%fill: #d4effc%' AND dist < {EPS} ORDER BY dist DESC
      )
    """)
    for row in sql():
        row1 = row[1].replace("'", "''")
        sql(f"UPDATE {lines} SET name = '{row1}', svgid = '{row[2]}' WHERE id = {row[0]}")

    LOGGER.info("Label any")
    sql(f"""
      SELECT t1id, substring(t2name for 1) || lower(substring(rtrim(t2name,'0123456789') from 2)),
        ltrim(t2name,'A''BCDEFGHIJKLMNOPQRSTUVWXYZ')
      FROM (
        SELECT t1.id AS t1id, substring(t2.name from 9) AS t2name, dist FROM {points} AS t1,
        LATERAL (
          SELECT t3.name AS name, ST_Distance(t1.wkb_geometry, t3.wkb_geometry) AS dist
          FROM {points} AS t3
          WHERE regexp_like(t3.name, 'AnyName/') AND
            NOT (t3.name LIKE '%000' OR t3.name LIKE '%500')
          ORDER BY ST_Distance(t1.wkb_geometry, t3.wkb_geometry) LIMIT 1
        )
        AS t2
        WHERE (
            t1.type = 'Abbey' OR
            t1.type LIKE 'BRIDGE%' OR
            t1.type LIKE 'Battle%' OR
            t1.type = 'Castle' OR
            t1.type LIKE 'Chapter%' OR
            t1.type = 'City' OR
            t1.type = 'Ferry' OR
            t1.type = 'Ford' OR
            t1.type LIKE '%Fort%' OR
            t1.type = 'Gargun' OR
            t1.type = 'Keep' OR
            t1.type = 'Mine' OR
            t1.type = 'PEAK' OR
            t1.type = 'Quarry' OR
            t1.type = 'Rapids' OR
            t1.type LIKE 'Ruin%' OR
            t1.type = 'Salt' OR
            t1.type LIKE 'Special%' OR
            t1.type LIKE '%Manor%' OR
            t1.type LIKE 'Tollbooth%' OR
            t1.type LIKE 'Tribal%' OR
            t1.type = 'Tunnel' OR
            t1.type = 'Waterfall'
          )
          AND dist < {EPS} AND (t1.name = '' OR t1.name = '-')
        ORDER BY dist DESC
      )
    """)
    for row in sql():
        row1 = row[1].replace("'", "''")
        sql(f"UPDATE {points} SET name = '{row1}', svgid = '{row[2]}' WHERE id = {row[0]}")

def duplicate_nonames(points):
    """Delete clear, but unnamed duplicates."""
    LOGGER.info("Delete unnamed duplicates")
    sql(f"""
      DELETE FROM {points} AS t0 USING (
        SELECT t1.id FROM {points} AS t1, {points} AS t2
        WHERE t1.id <> t2.id AND ST_Distance(t1.wkb_geometry, t2.wkb_geometry) < {EPSD}
          AND t1.name = '-' AND t2.name <> '-'
      ) AS t1 (id)
      WHERE t0.id = t1.id
    """)

def do_specials(points):
    """Manually detected."""
    LOGGER.info("Manual Additions")
    sql(f"""
      UPDATE {points} SET name = 'Varazal''s Wall'
      WHERE (type = '/CITIES/Kaldor/-' OR type = '/TYPE/Chybisa/-') AND
        ST_Covers(
          ST_Envelope(
            ST_Union('POINT(-17.218 44.033)'::geometry, 'POINT(-17.181 43.909)'::geometry)
          ),
          wkb_geometry
        )
    """)
    sql(f"""
      UPDATE {points} SET name = 'Maguda Falls', svgid = 120
      WHERE (type = '/TYPE/Rethem/-') AND
        ST_Covers(
          ST_Envelope(
            ST_Union('POINT(-25.154 44.766)'::geometry, 'POINT(-25.142 44.751)'::geometry)
          ),
          wkb_geometry
        )
    """)

def main(inpre):
    """Main method."""
    points = f"{inpre}_points"
    lines = f"{inpre}_lines"

    sql(f"SELECT count(*) FROM {points} WHERE name = '-'")
    LOGGER.info("Unnamed points...%s", sql()[0][0])

    obtain_names(points, lines)
    duplicate_nonames(points)
    do_specials(points)

    # Remaining
    sql(f"SELECT count(*) FROM {points} WHERE name = '-'")
    LOGGER.info("Remaining unnamed points...%s", sql()[0][0])
