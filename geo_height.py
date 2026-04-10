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
RSCALE = 250 # 400m pixels

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

def calc_bary(pts, diam):
    """Calculate barycentric elevation."""
    pzz = 0
    if diam > 0:
        # There is only the mesa boundary with diameter diam.  The
        # addition is bound by (1/2)/(1 + 1/2) = 1/3 => 333ft
        pzz = int(pts[0][0]) + 1000*(pts[0][1]/diam) / (1 + (pts[0][1]/diam))
    else:
        # SUM_i (hi/di) / SUM_j (1/dj), numerically stablized
        # SUM_i (hi / (1 + SUM_{i!=j} (di/dj)))
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
              SELECT ST_Covers(wkb_geometry, ST_GeomFromText('POINT({pts[0]} {pts[1]})'))
              FROM {args.table}_polys
              WHERE id = {pline['base']}
            """)
            result = cursor.fetchall()[0]
            if result[0]:
                peak_c = f"id IN ({','.join([str(p) for p in pline['peaks']])})"
                if peak_c.endswith('()'):
                    peak_c = "FALSE"
                hole_c = f"id IN ({','.join([str(p) for p in pline['holes']])})"
                if hole_c.endswith('()'):
                    hole_c = "FALSE"
                diam = 0
                if (peak_c == "FALSE") and (hole_c == "FALSE"):
                    cursor.execute(f"""
                      SELECT ST_MaxDistance(wkb_geometry, wkb_geometry)
                      FROM {args.table}_polys
                      WHERE id = {pline['base']}
                    """)
                    diam = float(cursor.fetchall()[0][0])
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
                return calc_bary(cursor.fetchall(), diam) * float(args.hscale)
    return 0

def create_raster(args, cursor):
    """Iterate over all heights and create a raster elevation field"""
    pt_lon = int(args.geo[0])
    pt_lat = int(args.geo[1])
    tb_id = 1000 * pt_lon + pt_lat
    matrix = numpy.full((RSCALE, RSCALE), 65535, numpy.float32)

    partitions = get_partitions(args, cursor, [pt_lon, pt_lat, pt_lon + 1, pt_lat + 1])
    pt_x = pt_lon
    ra_x = 0
    while ra_x < RSCALE:
        verbosity(args.verbose, f"longitude at {pt_x}")
        pt_y = pt_lat
        ra_y = 0
        while ra_y < RSCALE:
            matrix[ra_y][ra_x] = handle_partitions(args, cursor, partitions, [pt_x, pt_y])
            pt_y += 1 / RSCALE
            ra_y += 1
        pt_x += 1 / RSCALE
        ra_x += 1

    cursor.execute(f"""
      INSERT INTO xyz_heights (id, raster)
      SELECT {tb_id},
        ST_SetValues(
          ST_AddBand(
            ST_MakeEmptyRaster(
              {RSCALE}, {RSCALE},
              {pt_lon}, {pt_lat}, {1 / RSCALE}, {1 / RSCALE},
              0, 0
            ),
            '16BUI'::TEXT, 65535, 65535
          ),
          1, 1, 1, ARRAY{numpy.array2string(matrix, separator=",", threshold=numpy.inf)}::double precision[][]
        )
    """)

def write_geotiff(tb_id, pt_lon, pt_lat, matrix):
    """Write the raster into a GeoTIFF. Not used currently."""
    raster = rasterio.open(
        f"height{tb_id}.tif", height=RSCALE, width=RSCALE, driver="GTiff",
        count=1, nodata=65535, dtype='uint16', crs=None, mode="w",
        transform=rasterio.transform.Affine(1/RSCALE, 0, pt_lon, 0, 1/RSCALE, pt_lat, 0, 0, 1)
    )
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
        help='table prefix; _pts, _polys, and _lines will be added')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='verbose', required=False)
    parser.add_argument(
        '-g', '--geo', dest='geo',
        help='two (integer) geo coordinates: lon1 lat1', nargs=2, required=False)
    parser.add_argument(
        '-H', '--hscale', dest='hscale',
        help='float scale for db ft to out band value', required=True)
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
      CREATE TEMP SEQUENCE IF NOT EXISTS serial START 600000;
      CREATE TABLE IF NOT EXISTS xyz_heights (id integer primary key, raster raster);
      CREATE INDEX IF NOT EXISTS xyz_heights_raster_idx ON xyz_heights
        USING GIST(ST_Envelope(raster));
    """)
    # Constraints? SELECT AddRasterConstraints('xyz_heights'::name, 'raster'::name, ...);
    create_raster(args, cursor)
    conn.commit()

if __name__ == '__main__':
    main()
