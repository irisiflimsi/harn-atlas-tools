#!/usr/bin/python
"""
Creates some default heights.
"""
import random
import logging
import math
import inspect
import os.path
from enum import Enum
import numpy
import rasterio
import ext2pres
from ext2pres import sql

class Dir(Enum):
    """Direction."""
    LEFT = 1
    RIGHT = 2
    TOP = 3
    DOWN = 4

LOGGER = logging.getLogger(__name__)

# We do two things: we calculate 1km pixels exactly and randomize
# inbetween to give a more natural feel.  Since 1 degree is roughly
# 100km, 1km = 1/100 degrees. For fractals we need powers of two.
# RAS_SIZE = 1 / width of raster pixel in degrees.
RAS_SIZE = 256 # 100/256 km pixels
L2D = 16 # Local : Domain detail
D2A = 16 # Domain : Atlas detail
NO_DATA = 32767 # GeoTIF default
# Random heights
SPREAD_ATLAS = 500
SPREAD_DOMAIN = 256
SPREAD_LOCAL = 16

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

def calc_bary(pts, diam, fract):
    """Calculate barycentric elevation in feet."""
    # We include heuristic for fractals.  At larger distances the
    # elevation would converge to the average.  We use that to replace
    # the average with the fractal.  As we move into uncharted
    # territory, fractals take over.  fract is approximately in (-1,1)
    # and pts is ordered.
    hdiff = max(SPREAD_ATLAS, float(pts[-1][0]) - float(pts[0][0]))
    hd_pts = pts + [(fract * hdiff, 1/(len(pts) + 1))]
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

def handle_partitions(polygons, points, partitions, pts, fract):
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
                ret = calc_bary(sql_sorted, diam, fract)
                return ret
    return 0

def read(raster, raster_id, geo_pts, geo_scale):
    """Check whether TIF or DB raster is available. Copy."""
    matrix_tif = read_geotiff(raster_id, 1)
    matrix_sql = read_sqlraster(raster, raster_id, 1)
    if matrix_tif is None and matrix_sql is not None:
        displace = read_sqlraster(raster, raster_id, 2)
        write_geotiff((matrix_sql, displace), raster_id, geo_pts, geo_scale)
    if matrix_tif is not None and matrix_sql is None:
        displace = read_geotiff(raster_id, 2)
        write_sql(raster, (matrix_tif, displace), raster_id, geo_pts, geo_scale)
    return matrix_tif is not None or matrix_sql is not None

def write_sql(raster, fields, raster_id, geo_pts, geo_scale):
    """Write the raster to Postgis."""
    fields[0][fields[0] is None] = NO_DATA
    fields[1][fields[1] is None] = NO_DATA
    real_geo_scale = geo_scale * RAS_SIZE
    sql(f"""
      INSERT INTO {raster} (id, raster)
      SELECT '{raster_id}',
        ST_SetValues(
          ST_SetValues(
            ST_AddBand(
              ST_AddBand(
                ST_MakeEmptyRaster(
                  {RAS_SIZE + 1}, {RAS_SIZE + 1},
                  {geo_pts[0]}, {geo_pts[1]}, {1 / real_geo_scale}, {1 / real_geo_scale},
                  0, 0
                ),
                '16BSI'::TEXT, {NO_DATA}, {NO_DATA}
              ),
              '16BSI'::TEXT, {NO_DATA}, {NO_DATA}
            ),
            1, 1, 1, ARRAY{numpy.array2string(fields[0], separator=",", threshold=numpy.inf)}::double precision[][]
          ),
          2, 1, 1, ARRAY{numpy.array2string(fields[1], separator=",", threshold=numpy.inf)}::double precision[][]
        )
    """)

def write_geotiff(fields, raster_id, geo_pts, geo_scale):
    """Write the raster into a GeoTIFF. Not used currently."""
    real_geo_scale = geo_scale * RAS_SIZE
    patch = rasterio.open(
        f"height{raster_id}.tif", height=RAS_SIZE + 1, width=RAS_SIZE + 1,
        driver="GTiff", count=2, nodata=NO_DATA, dtype='int16', crs=None, mode="w",
        transform=rasterio.transform.Affine(
            1/real_geo_scale, 0, geo_pts[0], 0, 1/real_geo_scale, geo_pts[1], 0, 0, 1
        )
    )
    fields[0][fields[0] is None] = NO_DATA
    fields[1][fields[1] is None] = NO_DATA
    patch.write(fields[0], 1)
    patch.write(fields[1], 2)

def read_geotiff(raster_id, band):
    """Read the raster from GeoTIFF."""
    LOGGER.debug("Enter read %s band %s", f"height{raster_id}.tif", band)
    if not os.path.isfile(f"height{raster_id}.tif"):
        return None
    patch = rasterio.open(f"height{raster_id}.tif", mode="r")
    return numpy.asarray(patch.read(band))

def read_sqlraster(raster, raster_id, band):
    """Read the raster from Postgis."""
    sql(f"""
      SELECT ST_DumpValues(raster, {band})
      FROM {raster} WHERE id = '{raster_id}'
    """)
    result = sql()
    if len(result) < 1:
        return None
    return numpy.asarray(result[0][0])

def init_displace(displace, randf=random.uniform):
    """Initialize the (rest of the) displacement matrix."""
    if displace[0][0] == NO_DATA:
        displace[0][0] = 500 * randf(-1, 1)
    if displace[RAS_SIZE][0] == NO_DATA:
        displace[RAS_SIZE][0] = 500 * randf(-1, 1)
    if displace[0][RAS_SIZE] == NO_DATA:
        displace[0][RAS_SIZE] = 500 * randf(-1, 1)
    if displace[RAS_SIZE][RAS_SIZE] == NO_DATA:
        displace[RAS_SIZE][RAS_SIZE] = 500 * randf(-1, 1)
    fractal(displace, (0, 0), (RAS_SIZE, RAS_SIZE), 250, randf)
    return displace

def fractal(height, p_0, p_1, depth, randf):
    """Fractalize down. Include all corners."""
    p_t = (int((p_0[0] + p_1[0])/2), p_0[1])
    p_b = (int((p_0[0] + p_1[0])/2), p_1[1])
    p_l = (p_0[0], int((p_0[1] + p_1[1])/2))
    p_r = (p_1[0], int((p_0[1] + p_1[1])/2))
    p_m = (int((p_0[0] + p_1[0])/2), int((p_0[1] + p_1[1])/2))

    if height[p_t[0]][p_t[1]] == NO_DATA:
        height[p_t[0]][p_t[1]] = randf(-1, 1) * depth + \
            (height[p_0[0]][p_0[1]] + height[p_1[0]][p_0[1]])/2.0
    if height[p_b[0]][p_b[1]] == NO_DATA:
        height[p_b[0]][p_b[1]] = randf(-1, 1) * depth + \
            (height[p_0[0]][p_1[1]] + height[p_1[0]][p_1[1]])/2.0
    if height[p_l[0]][p_l[1]] == NO_DATA:
        height[p_l[0]][p_l[1]] = randf(-1, 1) * depth + \
            (height[p_0[0]][p_0[1]] + height[p_0[0]][p_1[1]])/2.0
    if height[p_r[0]][p_r[1]] == NO_DATA:
        height[p_r[0]][p_r[1]] = randf(-1, 1) * depth + \
            (height[p_1[0]][p_0[1]] + height[p_1[0]][p_1[1]])/2.0
    if height[p_m[0]][p_m[1]] == NO_DATA:
        height[p_m[0]][p_m[1]] = randf(-1, 1) * depth + \
            (height[p_t[0]][p_t[1]] + height[p_b[0]][p_b[1]])/4.0 + \
            (height[p_l[0]][p_l[1]] + height[p_r[0]][p_r[1]])/4.0

    # x-diff = y-diff
    if abs(p_0[0] - p_m[0]) >= 2:
        fractal(height, p_0, p_m, depth/2, randf)
    if abs(p_t[0] - p_r[0]) >= 2:
        fractal(height, p_t, p_r, depth/2, randf)
    if abs(p_l[0] - p_b[0]) >= 2:
        fractal(height, p_l, p_b, depth/2, randf)
    if abs(p_m[0] - p_1[0]) >= 2:
        fractal(height, p_m, p_1, depth/2, randf)

def eval_delta(array, i, j):
    """Maximum of existing array indices."""
    ret = 0
    for fun in (min, max):
        ret = - ret
        val = array[i, j]
        if len(array[i, :]) > j + 1:
            val = fun(val, array[i, j + 1])
        if len(array[:, j]) > i + 1:
            val = fun(val, array[i + 1, j])
        if len(array[i, :]) > j + 1 and len(array[:, j + 1]) > i + 1:
            val = fun(val, array[i + 1, j + 1])
        ret += val
    return ret

def weighted_average(array, idx, weight):
    """Weighted average of array at indices."""
    locw = weight[0]*weight[1]
    sumw = locw
    sumt = array[idx[0]][idx[1]]*locw
    if len(array[idx[0], :]) > idx[1] + 1:
        locw = weight[0]*(1 - weight[1])
        sumw += locw
        sumt += array[idx[0]][idx[1] + 1] * locw
    if len(array[:, idx[1]]) > idx[0] + 1:
        locw = (1 - weight[0])*weight[1]
        sumw += locw
        sumt += array[idx[0] + 1][idx[1]] * locw
    if len(array[idx[0], :]) > idx[1] + 1 and  len(array[:, idx[1]]) > idx[0] + 1:
        locw = (1 - weight[0])*(1 - weight[1])
        sumw += locw
        sumt += array[idx[0] + 1][idx[1] + 1] * locw
    return sumt / sumw

def neighbor_id(direction: Dir, pts):
    """Generate neighbor id."""
    cut = (D2A - 1, L2D - 1)
    if direction == Dir.LEFT:
        delta = (0, 0, 0, 0, -1, 0)
        if len(pts) == 6 and pts[4] == 0 or \
           len(pts) == 4:
            delta = (0, 0, -1, 0, L2D - 1, 0)
        if all(pts[2*i] == 0 for i in range(1, int(len(pts) / 2))):
            delta = (-1, 0, D2A - 1, 0, L2D - 1, 0)
    if direction == Dir.RIGHT:
        delta = (0, 0, 0, 0, 1, 0)
        if len(pts) == 6 and pts[4] == L2D - 1 or \
           len(pts) == 4:
            delta = (0, 0, 1, 0, 1 - L2D, 0)
        if all(pts[2*i] == cut[i - 1] for i in range(1, int(len(pts) / 2))):
            delta = (1, 0, 1 - D2A, 0, 1 - L2D, 0)
    if direction == Dir.DOWN:
        delta = (0, 0, 0, 0, 0, -1)
        if len(pts) == 6 and pts[5] == 0 or \
           len(pts) == 4:
            delta = (0, 0, 0, -1, 0, L2D - 1)
        if all(pts[2*i + 1] == 0 for i in range(1, int(len(pts) / 2))):
            delta = (0, -1, 0, D2A - 1, 0, L2D - 1)
    if direction == Dir.TOP:
        delta = (0, 0, 0, 0, 0, 1)
        if len(pts) == 6 and pts[5] == L2D - 1 or \
           len(pts) == 4:
            delta = (0, 0, 0, 1, 0, 1 - L2D)
        if all(pts[2*i + 1] == cut[i - 1] for i in range(1, int(len(pts) / 2))):
            delta = (0, 1, 0, 1 - D2A, 0, 1 - L2D)
    rpts = [sum(direction) for direction in zip(pts, delta)]
    rpts[0] = (rpts[0] + 180) % 360 - 180
    return ".".join([str(direction) for direction in rpts])

# Atlas, domain, and local have geo coordinates, that are prefixed
# geo.  Parsed coordinates are sorted x0,y0,x1,y1,..., where n
# subdivides the unit n-1.  These are simply 'pts'.  The rasters also
# have (local) raster coordinates that are prefixed ras.

def create_raster_local(rasters, lb_a, ru_a, pt_ad):
    """Iterate over all heights and create a local raster elevation field"""
    ptl_lon1 = int(lb_a[4]) if pt_ad[2] == int(lb_a[2]) else 0
    ptl_lat1 = int(lb_a[5]) if pt_ad[3] == int(lb_a[3]) else 0
    ptl_lon2 = int(ru_a[4]) if pt_ad[2] == int(ru_a[2]) else 0
    ptl_lat2 = int(ru_a[5]) if pt_ad[3] == int(ru_a[3]) else 0
    for ptl_lon in range(ptl_lon1, ptl_lon2 + 1):
        for ptl_lat in range(ptl_lat1, ptl_lat2 + 1):
            calc_local(rasters, [pt_ad[0], pt_ad[1], pt_ad[2], pt_ad[3], ptl_lon, ptl_lat])

def calc_local(rasters, pts):
    """Calculate the local-level height field."""
    local_id = ".".join([str(p) for p in pts[0:6]])
    geo_pts = (
        pts[0] + pts[2] / D2A + pts[4] / L2D / D2A,
        pts[1] + pts[3] / D2A + pts[5] / L2D / D2A
    )
    if read(rasters[2], local_id, geo_pts, L2D * D2A):
        LOGGER.info("Found %s", local_id)
        return
    LOGGER.info("Calculate %s", local_id)
    displace = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)

    # Prepare displace from already completed squares
    left = read_geotiff(neighbor_id(Dir.LEFT, pts[0:6]), 2)
    if left is not None:
        displace[:, 0] = left[:, RAS_SIZE]
    right = read_geotiff(neighbor_id(Dir.RIGHT, pts[0:6]), 2)
    if right is not None:
        displace[:, RAS_SIZE] = right[:, 0]
    down = read_geotiff(neighbor_id(Dir.DOWN, pts[0:6]), 2)
    if down is not None:
        displace[0, :] = down[RAS_SIZE, :]
    top = read_geotiff(neighbor_id(Dir.TOP, pts[0:6]), 2)
    if top is not None:
        displace[RAS_SIZE, :] = top[0, :]

    LOGGER.debug("left %s", displace[0, :])
    LOGGER.debug("right %s", displace[RAS_SIZE, :])
    LOGGER.debug("down %s", displace[:, 0])
    LOGGER.debug("top %s", displace[:, RAS_SIZE])

    # Fill remainder
    displace = init_displace(displace)
    LOGGER.debug("random %s", displace)

    domain_id = ".".join([str(p) for p in pts[0:4]])
    domain_field = read_geotiff(domain_id, 1)
    field = calc_localfield(domain_field, pts, displace)
    write_sql(rasters[2], (field, displace), local_id, geo_pts, L2D * D2A)
    write_geotiff((field, displace), local_id, geo_pts, L2D * D2A)

def calc_localfield(domain_field, pts, displace):
    """Calculate the domain height field."""
    LOGGER.info(
        "Make local raster starting at [%s,%s,%s,%s,%s,%s]",
        pts[0], pts[1], pts[2], pts[3], pts[4], pts[5]
    )
    field = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)
    geo_x = pts[0] + pts[2] / D2A + pts[4] / L2D / D2A
    ras_x = 0
    while ras_x < RAS_SIZE + 1:
        LOGGER.debug("longitude at %s", geo_x)
        ras_dx = int((geo_x - pts[0] - pts[2] / D2A) * D2A * RAS_SIZE)
        frac_lx = (geo_x - pts[0] - pts[2] / D2A) * D2A * RAS_SIZE - ras_dx
        geo_y = pts[1] + pts[3] / D2A + pts[5] / L2D / D2A
        ras_y = 0
        while ras_y < RAS_SIZE + 1:
            ras_dy = int((geo_y - pts[1] - pts[3] / D2A) * D2A * RAS_SIZE)
            frac_ly = (geo_y - pts[1] - pts[3] / D2A) * D2A * RAS_SIZE - ras_dy
            fract_h = displace[ras_y][ras_x] / 1000
            delta_af = eval_delta(domain_field, ras_dy, ras_dx)
            avg = weighted_average(domain_field, (ras_dy, ras_dx), (1 - frac_ly, 1 - frac_lx))
            field[ras_y][ras_x] = max(0, avg + fract_h * max(SPREAD_LOCAL, abs(delta_af)))
            geo_y += 1 / RAS_SIZE / L2D / D2A
            ras_y += 1
        geo_x += 1 / RAS_SIZE / L2D / D2A
        ras_x += 1
    return field

def create_raster_domain(rasters, lb_a, ru_a, pt_a):
    """Iterate over all heights and create a domain raster elevation field"""
    ptd_lon1 = int(lb_a[2]) if pt_a[0] == int(lb_a[0]) else 0
    ptd_lat1 = int(lb_a[3]) if pt_a[1] == int(lb_a[1]) else 0
    ptd_lon2 = int(ru_a[2]) if pt_a[0] == int(ru_a[0]) else 0
    ptd_lat2 = int(ru_a[3]) if pt_a[1] == int(ru_a[1]) else 0
    for ptd_lon in range(ptd_lon1, ptd_lon2 + 1):
        for ptd_lat in range(ptd_lat1, ptd_lat2 + 1):
            calc_domain(rasters, [pt_a[0], pt_a[1], ptd_lon, ptd_lat])
            if len(lb_a) > 4:
                create_raster_local(rasters, lb_a, ru_a, (pt_a[0], pt_a[1], ptd_lon, ptd_lat))

def calc_domain(rasters, pts):
    """Calculate the domain-level height field."""
    domain_id = ".".join([str(p) for p in pts[0:4]])
    geo_pts = (pts[0] + pts[2] / D2A, pts[1] + pts[3] / D2A)
    if read(rasters[1], domain_id, geo_pts, D2A):
        LOGGER.info("Found %s", domain_id)
        return
    LOGGER.info("Calculate %s", domain_id)
    displace = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)

    # Prepare displace from already completed squares
    left = read_geotiff(neighbor_id(Dir.LEFT, pts[0:4]), 2)
    if left is not None:
        displace[:, 0] = left[:, RAS_SIZE]
    right = read_geotiff(neighbor_id(Dir.RIGHT, pts[0:4]), 2)
    if right is not None:
        displace[0, RAS_SIZE] = right[:, 0]
    down = read_geotiff(neighbor_id(Dir.DOWN, pts[0:4]), 2)
    if down is not None:
        displace[0, :] = down[RAS_SIZE, :]
    top = read_geotiff(neighbor_id(Dir.TOP, pts[0:4]), 2)
    if top is not None:
        displace[RAS_SIZE, :] = top[0, :]

    LOGGER.debug("left %s", displace[0, :])
    LOGGER.debug("right %s", displace[RAS_SIZE, :])
    LOGGER.debug("down %s", displace[:, 0])
    LOGGER.debug("top %s", displace[:, RAS_SIZE])

    # Fill remainder
    displace = init_displace(displace)
    LOGGER.debug("random %s", displace)

    atlas_id = ".".join([str(p) for p in pts[0:2]])
    atlas_field = read_geotiff(atlas_id, 1)
    field = calc_domainfield(atlas_field, pts, displace)
    write_sql(rasters[1], (field, displace), domain_id, geo_pts, D2A)
    write_geotiff((field, displace), domain_id, geo_pts, D2A)

def calc_domainfield(atlas_field, pts, displace):
    """Calculate the domain height field."""
    LOGGER.info("Make domain raster for [%s,%s,%s,%s]", pts[0], pts[1], pts[2], pts[3])
    field = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)
    geo_x = pts[0] + pts[2] / D2A
    ras_x = 0
    while ras_x < RAS_SIZE + 1:
        LOGGER.debug("longitude at %s", geo_x)
        ras_ax = int((geo_x - pts[0]) * RAS_SIZE)
        frac_dx = (geo_x - pts[0]) * RAS_SIZE - ras_ax
        geo_y = pts[1] + pts[3] / D2A
        ras_y = 0
        while ras_y < RAS_SIZE + 1:
            ras_ay = int((geo_y - pts[1]) * RAS_SIZE)
            frac_dy = (geo_y - pts[1]) * RAS_SIZE - ras_ay
            fract_h = displace[ras_y][ras_x] / 1000
            delta_af = eval_delta(atlas_field, ras_ay, ras_ax)
            avg = weighted_average(atlas_field, (ras_ay, ras_ax), (1 - frac_dy, 1 - frac_dx))
            field[ras_y][ras_x] = max(0, avg + fract_h * max(SPREAD_DOMAIN, abs(delta_af)))
            geo_y += 1 / RAS_SIZE / D2A
            ras_y += 1
        geo_x += 1 / RAS_SIZE / D2A
        ras_x += 1
    return field

def create_raster_atlas(polygons, points, rasters, lb_a, ru_a):
    """Iterate over all heights and create an atlas raster elevation field"""
    if len(lb_a) == 2:
        level = "atlas"
    if len(lb_a) == 4:
        level = "domain"
    if len(lb_a) == 6:
        level = "local"
    LOGGER.info("Create raster on %s level", level)

    # Normalize
    lb_a[0] = (lb_a[0] + 180) % 360 - 180
    ru_a[0] = (ru_a[0] + 180) % 360 - 180
    for pta_lon in range(int(lb_a[0]), int(ru_a[0]) + 1):
        for pta_lat in range(int(lb_a[1]), int(ru_a[1]) + 1):
            calc_atlas(rasters, polygons, points, (pta_lon, pta_lat))
            if len(lb_a) > 2:
                create_raster_domain(rasters, lb_a, ru_a, (pta_lon, pta_lat))

def calc_atlas(rasters, polygons, points, pts):
    """Calculate the atlas-level height field."""
    atlas_id = ".".join([str(p) for p in pts[0:2]])
    geo_pts = (pts[0], pts[1])
    if read(rasters[0], atlas_id, geo_pts, 1):
        LOGGER.info("Found %s", atlas_id)
        return
    LOGGER.info("Calculate %s", atlas_id)
    displace = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)

    # Prepare displace from already completed squares
    left = read_geotiff(neighbor_id(Dir.LEFT, pts[0:2]), 2)
    if left is not None:
        displace[:, 0] = left[:, RAS_SIZE]
    right = read_geotiff(neighbor_id(Dir.RIGHT, pts[0:2]), 2)
    if right is not None:
        displace[:, RAS_SIZE] = right[:, 0]
    down = read_geotiff(neighbor_id(Dir.DOWN, pts[0:2]), 2)
    if down is not None:
        displace[0, :] = down[RAS_SIZE, :]
    top = read_geotiff(neighbor_id(Dir.TOP, pts[0:2]), 2)
    if top is not None:
        displace[RAS_SIZE, :] = top[0, :]

    LOGGER.debug("left %s", displace[0, :])
    LOGGER.debug("right %s", displace[RAS_SIZE, :])
    LOGGER.debug("down %s", displace[:, 0])
    LOGGER.debug("top %s", displace[:, RAS_SIZE])

    # Fill remainder
    displace = init_displace(displace)
    LOGGER.debug("random %s", displace)

    field = calc_atlasfield(polygons, points, pts, displace)
    write_sql(rasters[0], (field, displace), atlas_id, geo_pts, 1)
    write_geotiff((field, displace), atlas_id, geo_pts, 1)

def calc_atlasfield(polygons, points, pts, displace):
    """Calculate the atlas height field."""
    partitions = get_partitions(polygons, points, [pts[0], pts[1], pts[0] + 1, pts[1] + 1])
    field = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)
    LOGGER.info("Make atlas raster for [%s,%s]", pts[0], pts[1])
    geo_x = pts[0]
    ras_x = 0
    while ras_x < RAS_SIZE:
        LOGGER.debug("longitude at %s", geo_x)
        geo_y = pts[1]
        ras_y = 0
        while ras_y < RAS_SIZE:
            fract = displace[ras_y][ras_x] / 1000
            field[ras_y][ras_x] = max(
                0,
                handle_partitions(
                    polygons, points, partitions, [geo_x, geo_y], fract
                )
            )
            geo_y += 1 / RAS_SIZE
            ras_y += 1
        geo_x += 1 / RAS_SIZE
        ras_x += 1
    return field

def main(inpre, outpre, lb_a, ru_a):
    """Main method."""

    rasters = [f"{outpre}_atlas_rasters", f"{outpre}_domain_rasters", f"{outpre}_local_rasters"]
    polygons = f"{inpre}_polygons"
    points = f"{inpre}_points"

    # Initialize

    # I have to commit these command separately, otherwise psycopg2
    # will hang.
    sql(f"CREATE TEMP SEQUENCE IF NOT EXISTS serial START 600000")
    ext2pres.CONNECT.commit()
    # Constraints? SELECT AddRasterConstraints('{raster}'::name, 'raster'::name, ...);

    create_raster_atlas(polygons, points, rasters, lb_a, ru_a)

def tests():
    """Tests."""
    fractal_tests()
    neighbor_tests1()
    neighbor_tests2()
    neighbor_tests3()
    interpolation_tests1()
    interpolation_tests2()
    interpolation_tests3()
    side_tests1()
    side_tests2()

def fractal_tests():
    """Fractal simple tests."""
    num_tests = 0

    num_tests += 1
    field = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)
    field = init_displace(field, lambda x, y: x)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert (field[i][j] >= -1164.0625 and field[i][j] <= 1164.0625)
    assert any([
        field[i][j] > 1160 or field[i][j] < -1160
        for i in range(0, RAS_SIZE + 1) for j in range(0, RAS_SIZE + 1)
    ])

    num_tests += 1
    field = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)
    field = init_displace(field, lambda x, y: y)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert (field[i][j] >= -1164.0625 and field[i][j] <= 1164.0625)
    assert any([
        field[i][j] > 1160 or field[i][j] < -1160
        for i in range(0, RAS_SIZE + 1) for j in range(0, RAS_SIZE + 1)
    ])

    num_tests += 1
    field = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), NO_DATA, numpy.float32)
    field = init_displace(field, lambda x, y: 0)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert field[i][j] == 0

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def neighbor_tests1():
    """Neighbor_id arithmetic atlas."""
    num_tests = 0

    num_tests += 1
    assert neighbor_id(Dir.LEFT, (100, 0)) == "99.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (100, 0)) == "101.0"
    num_tests += 1
    assert neighbor_id(Dir.LEFT, (-100, 0)) == "-101.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (-100, 0)) == "-99.0"
    num_tests += 1
    assert neighbor_id(Dir.LEFT, (350, 0)) == "-11.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (350, 0)) == "-9.0"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, 50)) == "0.51"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, 50)) == "0.49"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, -50)) == "0.-49"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, -50)) == "0.-51"
    num_tests += 1
    assert neighbor_id(Dir.LEFT, (0, 0)) == "-1.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (0, 0)) == "1.0"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, 0)) == "0.1"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, 0)) == "0.-1"

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def neighbor_tests2():
    """Neighbor_id arithmetic domain."""
    num_tests = 0

    num_tests += 1
    assert neighbor_id(Dir.LEFT, (0, 0, 0, 0)) == "-1.0.15.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (0, 0, 0, 0)) == "0.0.1.0"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, 0, 0, 0)) == "0.0.0.1"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, 0, 0, 0)) == "0.-1.0.15"
    num_tests += 1
    assert neighbor_id(Dir.LEFT, (0, 0, 15, 0)) == "0.0.14.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (0, 0, 15, 0)) == "1.0.0.0"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, 0, 0, 15)) == "0.1.0.0"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, 0, 0, 15)) == "0.0.0.14"

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def neighbor_tests3():
    """Neighbor_id arithmetic local."""
    num_tests = 0

    num_tests += 1
    assert neighbor_id(Dir.LEFT, (0, 0, 0, 0, 0, 0)) == "-1.0.15.0.15.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (0, 0, 0, 0, 0, 0)) == "0.0.0.0.1.0"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, 0, 0, 0, 0, 0)) == "0.0.0.0.0.1"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, 0, 0, 0, 0, 0)) == "0.-1.0.15.0.15"
    num_tests += 1
    assert neighbor_id(Dir.LEFT, (0, 0, 15, 0, 0, 0)) == "0.0.14.0.15.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (0, 0, 15, 0, 0, 0)) == "0.0.15.0.1.0"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, 0, 0, 15, 0, 0)) == "0.0.0.15.0.1"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, 0, 0, 15, 0, 0)) == "0.0.0.14.0.15"
    num_tests += 1
    assert neighbor_id(Dir.LEFT, (0, 0, 0, 0, 15, 0)) == "0.0.0.0.14.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (0, 0, 0, 0, 15, 0)) == "0.0.1.0.0.0"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, 0, 0, 0, 0, 15)) == "0.0.0.1.0.0"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, 0, 0, 0, 0, 15)) == "0.0.0.0.0.14"
    num_tests += 1
    assert neighbor_id(Dir.LEFT, (0, 0, 15, 0, 15, 0)) == "0.0.15.0.14.0"
    num_tests += 1
    assert neighbor_id(Dir.RIGHT, (0, 0, 15, 0, 15, 0)) == "1.0.0.0.0.0"
    num_tests += 1
    assert neighbor_id(Dir.TOP, (0, 0, 0, 15, 0, 15)) == "0.1.0.0.0.0"
    num_tests += 1
    assert neighbor_id(Dir.DOWN, (0, 0, 0, 15, 0, 15)) == "0.0.0.15.0.14"

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def interpolation_tests1():
    """Interpolation on domain level."""
    displace = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # i domain
    num_tests += 1
    atlas = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            atlas[j, i] = i
    domain = calc_domainfield(atlas, (0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(domain[j][i], i/16)

    # j domain
    num_tests += 1
    atlas = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            atlas[j, i] = j
    domain = calc_domainfield(atlas, (0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(domain[j][i], j/16)

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def interpolation_tests2():
    """Interpolation on domain and local level."""
    displace = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # i*j domain
    num_tests += 1
    atlas = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            atlas[j, i] = i*j
    domain = calc_domainfield(atlas, (0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(domain[j][i], i/16 * j/16)

    # i + j local
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            domain[j, i] = j+i
    local = calc_localfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(local[j][i], i/16 + j/16)

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def interpolation_tests3():
    """Interpolation on local level."""
    displace = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # i local
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            domain[j, i] = i
    local = calc_localfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(local[j][i], i/16)

    # j local
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            domain[j, i] = j
    local = calc_localfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(local[j][i], j/16)

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def side_tests1():
    """Neighbor effects."""
    displace = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # left local
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    domain[0, :] = 1
    local = calc_localfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(local[j][i], max(0, 1 - j/16))

    # right local
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    domain[RAS_SIZE, :] = 1
    local = calc_localfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(local[j][i], max(0, j/16 - RAS_SIZE + 16))

    #  down local
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    domain[:, 0] = 1
    local = calc_localfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(local[j][i], max(0, 1 - i/16))

    #  top local
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    domain[:, RAS_SIZE] = 1
    local = calc_localfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(local[j][i], max(0, i/16 - RAS_SIZE + 16))

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def side_tests2():
    """Neighbor effects."""
    displace = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # left domain
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    domain[0, :] = 1
    domain = calc_domainfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(domain[j][i], max(0, 1 - j/16))

    # right domain
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    domain[RAS_SIZE, :] = 1
    domain = calc_domainfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(domain[j][i], max(0, j/16 - RAS_SIZE + 16))

    #  down domain
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    domain[:, 0] = 1
    domain = calc_domainfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(domain[j][i], max(0, 1 - i/16))

    #  top domain
    num_tests += 1
    domain = numpy.full((RAS_SIZE + 1, RAS_SIZE + 1), 0, numpy.float32)
    domain[:, RAS_SIZE] = 1
    domain = calc_domainfield(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RAS_SIZE + 1):
        for j in range(0, RAS_SIZE + 1):
            assert math.isclose(domain[j][i], max(0, i/16 - RAS_SIZE + 16))

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")
