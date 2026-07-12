#!/usr/bin/python
"""
Constructor artifact. See __main__.py for package docs.
"""
import psycopg2

DBC = "user:password@dbname:localhost:25432"

CONNECT = psycopg2.connect(
    user=f"{DBC.split('@')[0].split(':')[0]}",
    password=f"{DBC.split('@')[0].split(':')[1]}",
    database=f"{DBC.split('@')[1].split(':')[0]}",
    host=f"{DBC.split('@')[1].split(':')[1]}",
    port=f"{DBC.split('@')[1].split(':')[2]}"
)

CURSOR = CONNECT.cursor()

def sql(*args):
    """Shorthand for execute and fetchall."""
    if len(args) > 0:
        CURSOR.execute(args[0])
        return None
    return CURSOR.fetchall()

def shortest_connect(table, line_id, line_type, eps):
    """
    Returns the id of the closest line, the type, the geometry of it,
    of the original line, and of the connecting line.
    """
    sql(f"SELECT wkb_geometry FROM {table} WHERE id = {line_id}")
    line_geo = sql()[0][0]

    p_11 = f"(1, ST_StartPoint('{line_geo}'::geometry))"
    p_12 = f"(2, ST_EndPoint('{line_geo}'::geometry))"
    p_21 = f"(1, ST_StartPoint(main.wkb_geometry))"
    p_22 = f"(2, ST_EndPoint(main.wkb_geometry))"

    sql(f"""
      SELECT add_id, add_type, add_geo, line_geo, connect_geo FROM (
        SELECT main.id, main.type, main.wkb_geometry, '{line_geo}', (
          WITH pts1 (i, p) AS (VALUES {p_11}, {p_12}), 
            pts2 (i, p) AS (VALUES {p_21}, {p_22})
          SELECT ST_MakeLine(pt1.p, pt2.p) FROM pts1 AS pt1 CROSS JOIN pts2 AS pt2
          WHERE (main.id <> {line_id} OR pt1.i <> pt2.i)
          ORDER BY ST_Distance(pt1.p, pt2.p) ASC LIMIT 1
        )
        FROM {table} AS main
      )
      AS connects (add_id, add_type, add_geo, line_geo, connect_geo)
      WHERE ST_Length(connects.connect_geo) < {eps} AND (
        connects.add_type LIKE '%COASTLINE%' OR connects.add_type = '{line_type}'
      )
      ORDER BY ST_Length(connects.connect_geo) ASC LIMIT 1
    """)
    return sql()
