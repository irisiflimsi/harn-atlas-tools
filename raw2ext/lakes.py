#!/usr/bin/python
"""
Evaluates the lakes on the map. Assumes elevations to have been
done.
"""
import logging
from raw2ext import sql

LOGGER = logging.getLogger(__name__)

def main(inpre):
    """Main method."""

    lines = f"{inpre}_lines"

    # Initialize
    sql(f"""
      ALTER TABLE {lines} ALTER id SET NOT NULL;
      SELECT id, wkb_geometry FROM {lines} WHERE type LIKE '%LAKES%'
    """)
    LOGGER.info("Identifying lines: %s", sql()[0][0])

    # All colored closed lines are lakes
    LOGGER.info("Elevate all lakes")
    sql(f"""
      UPDATE {lines} SET type = 'Lake'
      WHERE ST_IsClosed(wkb_geometry) AND type LIKE '%LAKES%' AND style LIKE '%fill: #d4effc%'
    """)

    sql(f"""
      UPDATE {lines} AS t1 SET type = 'Lake',
        wkb_geometry = (
          SELECT ST_AddPoint(geo, ST_StartPoint(geo))
          FROM (
            SELECT (ST_Dump(ST_Node(wkb_geometry))).geom AS geo
            FROM {lines} AS t2
            WHERE t1.id = t2.id AND t2.style LIKE '%fill: #d4effc%'
          )
          ORDER BY ST_Length(geo) DESC LIMIT 1
        )
      WHERE type LIKE '%LAKES%' AND style LIKE '%fill: #d4effc%'
    """)
