#!/usr/bin/python
"""
Creates some default heights.
"""
import sys
import argparse
import psycopg2
import rasterio
import numpy

# (One over) Width of raster pixel in degrees.  Since 1 degree is
# roughly 100km, 100m is roughly 1/1000 degrees.
RSCALE = 2000 # 50m pixels

def get_partitions(args, cursor, pts):
    """Get partition of covered area."""
    # partitions[lvl][{base:id,holes:[holes],peaks:[peaks]}]
    partitions = []
    level = 0
    while True:
        # Base
        cursor.execute(f"""
          SELECT id, ST_AsText(wkb_geometry) FROM {args.table}_polys
          WHERE type = '{500*level}' AND
            ST_Intersects(wkb_geometry, ST_MakeEnvelope({pts[0]}, {pts[1]}, {pts[2]}, {pts[3]}))
        """)
        base_list = []
        found_list = cursor.fetchall()
        for found in found_list:
            # Holes
            cursor.execute(f"""
              SELECT id, ST_AsText(wkb_geometry) FROM {args.table}_polys
              WHERE type = '{500*level + 500}' AND
                ST_Covers('{found[1]}'::geometry, wkb_geometry)
            """)
            holes = cursor.fetchall()
            # Peaks
            cursor.execute(f"""
              SELECT id FROM {args.table}_pts
              WHERE type = 'PEAK' AND
                ST_Covers('{found[1]}'::geometry, wkb_geometry)
            """)
            peaks = [p[0] for p in cursor.fetchall()]
            for hole in holes:
                cursor.execute(f"""
                  SELECT id FROM {args.table}_pts
                  WHERE type = 'PEAK' AND
                    ST_Covers('{hole[1]}'::geometry, wkb_geometry)
                """)
                peaks = list(set(peaks) - {d[0] for d in cursor.fetchall()})
            base_list.append({"base": found[0], "holes": [h[0] for h in holes], "peaks": peaks})
        if len(found_list) == 0:
            break
        partitions.append(base_list)
        level += 1
    return partitions

def calc_bary(pts):
    """Calculate barycentric elevation."""
    # SUM_i (hi/di) / SUM_j (1/dj), numerically stablized
    # SUM_i (hi / (1 + SUM_{i!=j} (di/dj)))
    pzz = 0
    for i, pti in enumerate(pts):
        pzzin = int(pti[0])
        pzzid = 1
        for j, ptj in enumerate(pts):
            if i != j:
                pzzid += pti[1] / ptj[1]
        pzz += pzzin / pzzid
    return pzz

def handle_partitions(args, cursor, partitions, pts):
    """Handle all partitions for a single point."""
    # Matching is top down, thereby we don't have to evaluate holes.
    for partition in reversed(partitions):
        for pline in partition:
            cursor.execute(f"""
              SELECT ST_Covers(wkb_geometry,  ST_GeomFromText('POINT({pts[0]} {pts[1]})'))
              FROM {args.table}_polys
              WHERE id = {pline['base']}
            """)
            if cursor.fetchall()[0][0]:
                peak_c = f"id IN ({','.join([str(p) for p in pline['peaks']])})"
                if peak_c.endswith('()'):
                    peak_c = "FALSE"
                hole_c = f"id IN ({','.join([str(p) for p in pline['holes']])})"
                if hole_c.endswith('()'):
                    hole_c = "FALSE"
                cursor.execute(f"""
                  SELECT peak.svgid,
                    ST_Distance(ST_GeomFromText('POINT({pts[0]} {pts[1]})'), peak.wkb_geometry)
                  FROM {args.table}_pts AS peak
                  WHERE {peak_c}
                  UNION ALL
                  SELECT line.type,
                    ST_Distance(ST_GeomFromText('POINT({pts[0]} {pts[1]})'), ST_Boundary(line.wkb_geometry))
                  FROM {args.table}_polys AS line
                  WHERE id = {pline['base']} OR {hole_c}
                """)
                # Mapping of 2000ft to 64000 for simplicity.
                # Logically, a 2D pixel unit is 50m (see above), so for
                # the vertical unit the factor for pzz (in ft) should
                # be 1 / 3ft/m / 50m.
                return calc_bary(cursor.fetchall()) * 32
    return 0

def create_raster(args, cursor):
    """Iterate over all heights and create a raster elevation field"""
    ptx1 = -16.7
    ptx2 = -16.5
    pty1 = 40.35
    pty2 = 40.55
    ra_w = int((ptx2 - ptx1) * RSCALE)
    ra_h = int((pty2 - pty1) * RSCALE)
    matrix = numpy.full((ra_h, ra_w), 65535, numpy.float32)
    raster = rasterio.open(
        "all.tif", height=ra_h, width=ra_w, driver="GTiff",
        count=1, nodata=65535, dtype='uint16', crs=None, mode="w",
        transform=rasterio.transform.Affine(1/RSCALE, 0, ptx1, 0, 1/RSCALE, pty1, 0, 0, 1)
    )

    partitions = get_partitions(args, cursor, [ptx1, pty1, ptx2, pty2])

    ptx = ptx1
    ra_x = 0
    while ra_x < ra_w:
        verbosity(args.verbose, f"longitude at {ptx}")
        pty = pty1
        ra_y = 0
        while ra_y < ra_h:
            matrix[ra_y][ra_x] = handle_partitions(args, cursor, partitions, [ptx, pty])
            pty += 1 / RSCALE
            ra_y += 1
        ptx += 1 / RSCALE
        ra_x += 1
    raster.write(matrix, 1)

def verbosity(verb, out):
    """Verbosity."""
    if verb:
        print(f" - {out}")

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
    args = parser.parse_args()

    conn = psycopg2.connect(
        user=f"{args.db.split('@')[0].split(':')[0]}",
        password=f"{args.db.split('@')[0].split(':')[1]}",
        database=f"{args.db.split('@')[1].split(':')[0]}",
        host=f"{args.db.split('@')[1].split(':')[1]}",
        port=f"{args.db.split('@')[1].split(':')[2]}")
    cursor = conn.cursor()

    # Initialize
    create_raster(args, cursor)

if __name__ == '__main__':
    main()
