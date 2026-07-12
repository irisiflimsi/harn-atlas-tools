#!/usr/bin/python
"""
Creates some default heights.
"""
import random
import logging
import rasterio
import numpy
import ext2pres
from ext2pres import sql

LOGGER = logging.getLogger(__name__)

# We do two things: we calculate 1km pixels exactly and randomize
# inbetween to give a more natural feel.  Since 1 degree is roughly
# 100km, 1km = 1/100 degrees. For fractals we need powers of two.
# RSCALE = 1 / width of raster pixel in degrees.
RSCALE = 256 # 100/256 km pixels

def get_partitions(polygons, points, pts):
    """Get partition of covered area."""
    # partitions[lvl][{base:id,holes:[holes],peaks:[peaks]}]
    partitions = []
    level = 0
    while True:
        # Base
        sql(f"""
          SELECT id, ST_AsText(wkb_geometry) FROM {polygons}
          WHERE type = '{500*level}' AND
            ST_Intersects(wkb_geometry, ST_MakeEnvelope({pts[0]}, {pts[1]}, {pts[2]}, {pts[3]}))
        """)
        base_list = []
        found_list = sql()
        for found in found_list:
            # Holes
            sql(f"""
              SELECT id, ST_AsText(wkb_geometry) FROM {polygons}
              WHERE type = '{500*level + 500}' AND
                ST_Covers('{found[1]}'::geometry, wkb_geometry)
            """)
            holes = sql()
            # Peaks
            sql(f"""
              SELECT id FROM {points}
              WHERE type = 'PEAK' AND svgid ~ '^[0-9]+$' AND
                ST_Covers('{found[1]}'::geometry, wkb_geometry)
            """)
            peaks = [p[0] for p in sql()]
            for hole in holes:
                sql(f"""
                  SELECT id FROM {points}
                  WHERE type = 'PEAK' AND
                    ST_Covers('{hole[1]}'::geometry, wkb_geometry)
                """)
                peaks = list(set(peaks) - {d[0] for d in sql()})
            base_list.append({"base": found[0], "holes": [h[0] for h in holes], "peaks": peaks})
        if len(found_list) == 0:
            break
        partitions.append(base_list)
        level += 1
    return partitions

def calc_bary(pts, diam, fract_h):
    """Calculate barycentric elevation in feet."""
    # We include heuristic for fractals.  At larger distances the
    # elevation would converge to the average.  We use that to replace
    # the average with the fractal.  As we move into uncharted
    # territory, fractals take over.  Input fract_h is in [-1/2,1/2]
    # and pts is ordered.
    hdiff = float(pts[-1][0]) - float(pts[0][0])
    hd_pts = pts + [(hdiff * fract_h, 1/(len(pts) + 1))]
    hsum = 0
    dsum = 0
    if diam > 0:
        # There is only the mesa boundary with diameter diam.  The
        # addition is bound by (1/2)/(1 + 1/2) = 1/3 => 333ft
        hsum = int(hd_pts[0][0]) + 1000*(hd_pts[0][1]/diam) / (1 + (hd_pts[0][1]/diam))
    else:
        # SUM_i (hi/di) / SUM_j (1/dj) including fract
        for hdj in hd_pts:
            if float(hdj[1]) != 0:
                dsum += 1/float(hdj[1])
        for hdi in hd_pts:
            if float(hdi[1]) != 0:
                hsum += float(hdi[0]) / float(hdi[1]) / dsum
            else:
                hsum = float(hdi[0])
                break
    return hsum

def handle_partitions(polygons, points, partitions, pts, fract_h):
    """Handle all partitions for a single point."""
    # Matching is top down, thereby we don't have to evaluate holes.
    for partition in reversed(partitions):
        for pline in partition:
            sql(f"""
              SELECT ST_Covers(wkb_geometry, ST_GeomFromText('POINT({pts[0]} {pts[1]})'))
              FROM {polygons}
              WHERE id = {pline['base']}
            """)
            result = sql()[0]
            if result[0]:
                peak_c = f"id IN ({','.join([str(p) for p in pline['peaks']])})"
                if peak_c.endswith('()'):
                    peak_c = "FALSE"
                hole_c = f"id IN ({','.join([str(p) for p in pline['holes']])})"
                if hole_c.endswith('()'):
                    hole_c = "FALSE"
                diam = 0
                if (peak_c == "FALSE") and (hole_c == "FALSE"):
                    sql(f"""
                      SELECT ST_MaxDistance(wkb_geometry, wkb_geometry)
                      FROM {polygons}
                      WHERE id = {pline['base']}
                    """)
                    diam = float(sql()[0][0])
                sql(f"""
                  SELECT peak.svgid,
                    ST_Distance(ST_GeomFromText('POINT({pts[0]} {pts[1]})'), peak.wkb_geometry)
                  FROM {points} AS peak
                  WHERE {peak_c}
                  UNION ALL
                  SELECT line.type,
                    ST_Distance(
                      ST_GeomFromText('POINT({pts[0]} {pts[1]})'), ST_Boundary(line.wkb_geometry)
                    )
                  FROM {polygons} AS line
                  WHERE id = {pline['base']} OR {hole_c}
                """)
                sql_sorted = sorted(sql(), key=lambda x: float(x[0]))
                ret = calc_bary(sql_sorted, diam, fract_h) * float(166.7 / 500.0) # metric 3ft = 1m
                return ret
    return 0

def create_raster(polygons, points, raster):
    """Iterate over all heights and create a raster elevation field"""
    for pt_lon in range(-17, -15): # (-30, -15):
        for pt_lat in range(40, 42): # (40, 50):
            tb_id = 1000 * pt_lon + pt_lat
            displace = numpy.full((RSCALE + 1, RSCALE + 1), -1, numpy.float32)
            for i in range(0, RSCALE+1):
                displace[i][0] = displace[i][RSCALE] = RSCALE
                displace[0][i] = displace[RSCALE][i] = RSCALE
            fractal(displace, (0, 0), (RSCALE, RSCALE), RSCALE/2)
            matrix = numpy.full((RSCALE, RSCALE), 65535, numpy.float32)
            partitions = get_partitions(polygons, points, [pt_lon, pt_lat, pt_lon + 1, pt_lat + 1])
            pt_x = pt_lon
            ra_x = 0
            LOGGER.info("Make raster for [%s,%s]x[%s,%s]", pt_lon, pt_lon + 1, pt_lat, pt_lat + 1)
            while ra_x < RSCALE:
                LOGGER.debug("longitude at %s", pt_x)
                pt_y = pt_lat
                ra_y = 0
                while ra_y < RSCALE:
                    fract_h = displace[ra_y][ra_x]/RSCALE - 1
                    matrix[ra_y][ra_x] = max(
                        0,
                        handle_partitions(
                            polygons, points, partitions, [pt_x, pt_y], fract_h
                        )
                    )
                    pt_y += 1 / RSCALE
                    ra_y += 1
                pt_x += 1 / RSCALE
                ra_x += 1

            sql(f"""
              INSERT INTO {raster} (id, raster)
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
            write_geotiff(tb_id, pt_lon, pt_lat, matrix)

def write_geotiff(tb_id, pt_lon, pt_lat, matrix):
    """Write the raster into a GeoTIFF. Not used currently."""
    patch = rasterio.open(
        f"height{tb_id}.tif", height=RSCALE, width=RSCALE, driver="GTiff",
        count=1, nodata=65535, dtype='uint16', crs=None, mode="w",
        transform=rasterio.transform.Affine(1/RSCALE, 0, pt_lon, 0, 1/RSCALE, pt_lat, 0, 0, 1)
    )
    patch.write(matrix, 1)

def fractal(height, p_0, p_1, depth):
    """Fractalize down."""
    p_t = (int((p_0[0] + p_1[0])/2), p_0[1])
    p_b = (int((p_0[0] + p_1[0])/2), p_1[1])
    p_l = (p_0[0], int((p_0[1] + p_1[1])/2))
    p_r = (p_1[0], int((p_0[1] + p_1[1])/2))
    p_m = (int((p_0[0] + p_1[0])/2), int((p_0[1] + p_1[1])/2))

    if height[p_t[0]][p_t[1]] == -1:
        height[p_t[0]][p_t[1]] = random.uniform(-1, 1) * depth + \
            (height[p_0[0]][p_0[1]] + height[p_1[0]][p_0[1]])/2.0
    if height[p_b[0]][p_b[1]] == -1:
        height[p_b[0]][p_b[1]] = random.uniform(-1, 1) * depth + \
            (height[p_0[0]][p_1[1]] + height[p_1[0]][p_1[1]])/2.0
    if height[p_l[0]][p_l[1]] == -1:
        height[p_l[0]][p_l[1]] = random.uniform(-1, 1) * depth + \
            (height[p_0[0]][p_0[1]] + height[p_0[0]][p_1[1]])/2.0
    if height[p_r[0]][p_r[1]] == -1:
        height[p_r[0]][p_r[1]] = random.uniform(-1, 1) * depth + \
            (height[p_1[0]][p_0[1]] + height[p_1[0]][p_1[1]])/2.0
    if height[p_m[0]][p_m[1]] == -1:
        height[p_m[0]][p_m[1]] = random.uniform(-1, 1) * depth + \
            (height[p_t[0]][p_t[1]] + height[p_b[0]][p_b[1]])/4.0 + \
            (height[p_l[0]][p_l[1]] + height[p_r[0]][p_r[1]])/4.0

    if depth > 1:
        fractal(height, p_0, p_m, depth/2)
        fractal(height, p_t, p_r, depth/2)
        fractal(height, p_l, p_b, depth/2)
        fractal(height, p_m, p_1, depth/2)

def main(inpre, outpre):
    """Main method."""

    raster = f"{outpre}_atlas_rasters"
    polygons = f"{inpre}_polygons"
    points = f"{inpre}_points"

    # Initialize

    # I have to commit these command separately, otherwise psycopg2
    # will hang.
    sql(f"CREATE TEMP SEQUENCE IF NOT EXISTS serial START 600000")
    ext2pres.CONNECT.commit()
    sql(f"CREATE TABLE IF NOT EXISTS {raster} (id integer primary key, raster raster)")
    ext2pres.CONNECT.commit()
    sql(f"""
      CREATE INDEX IF NOT EXISTS {raster}_raster_idx ON {raster}
        USING GIST(ST_Envelope(raster));
    """)
    ext2pres.CONNECT.commit()
    # Constraints? SELECT AddRasterConstraints('{raster}'::name, 'raster'::name, ...);

    create_raster(polygons, points, raster)
