#!/usr/bin/python
"""
Creates some default heights.
"""
import random
import logging
import math
import inspect
from enum import Enum
import numpy
import ext2pres
from ext2pres import sql, calc_bary, read_geotiff, write_geotiff, write_sql, read_sql
from ext2pres import NO_DATA, RASTER_SIZE, L2D, D2A

class Dir(Enum):
    """Direction."""
    LEFT = 1
    RIGHT = 2
    TOP = 3
    DOWN = 4

# Height random deviation. Consider local show 10ft, atlas 500ft
# contours.
ATLAS_DEV_H = 128
DOMAIN_DEV_H = 32
LOCAL_DEV_H = 4

LOGGER = logging.getLogger(__name__)

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

def handle_partitions(polygons, points, partitions, pts):
    """Handle all partitions for a single point."""
    # Matching is top down, thereby we don't have to evaluate holes.
    for partition in reversed(partitions):
        for pline in partition:
            sql(f"""
              SELECT ST_Covers(wkb_geometry, 'POINT({pts[0]} {pts[1]})'::geometry)
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
                    ST_Distance('POINT({pts[0]} {pts[1]})'::geometry, peak.wkb_geometry)
                  FROM {points} AS peak
                  WHERE {peak_c}
                  UNION ALL
                  SELECT line.type,
                    ST_Distance(
                      'POINT({pts[0]} {pts[1]})'::geometry, ST_Boundary(line.wkb_geometry)
                    )
                  FROM {polygons} AS line
                  WHERE id = {pline['base']} OR {hole_c}
                """)
                sql_sorted = sorted(sql(), key=lambda x: float(x[0]))
                ret = calc_bary(sql_sorted, diam, ATLAS_DEV_H)
                return ret
    # Do sub-waterline minimum available
    if len(partitions) == 0:
        return 0
    depth = -13200
    for pline in partitions[0]:
        sql(f"""
          SELECT line.type,
            ST_Distance(
              'POINT({pts[0]} {pts[1]})'::geometry, ST_Boundary(line.wkb_geometry)
            )
          FROM {polygons} AS line
          WHERE id = {pline['base']}
          UNION
          VALUES ('-13200', 1)
        """)
        sql_sorted = sorted(sql(), key=lambda x: float(x[0]))
        depth = max(depth, calc_bary(sql_sorted, 0, - ATLAS_DEV_H))
    return depth

def read(raster, raster_id, geo_pts, geo_scale):
    """Check whether TIF or DB raster is available. Copy."""
    matrix_tif = read_geotiff(raster_id, 1)
    matrix_sql = read_sql(raster, raster_id, 1)
    if matrix_tif is None and matrix_sql is not None:
        displace = read_sql(raster, raster_id, 2)
        write_geotiff((matrix_sql, displace), raster_id, geo_pts, geo_scale)
    if matrix_tif is not None and matrix_sql is None:
        displace = read_geotiff(raster_id, 2)
        write_sql(raster, (matrix_tif, displace), raster_id, geo_pts, geo_scale)
    return matrix_tif is not None or matrix_sql is not None

def init_displace(displace, randf=random.uniform, rseed=0):
    """Initialize the (rest of the) displacement matrix."""
    random.seed(rseed)
    if displace[0][0] == NO_DATA:
        displace[0][0] = 500 * randf(-1, 1)
    if displace[RASTER_SIZE][0] == NO_DATA:
        displace[RASTER_SIZE][0] = 500 * randf(-1, 1)
    if displace[0][RASTER_SIZE] == NO_DATA:
        displace[0][RASTER_SIZE] = 500 * randf(-1, 1)
    if displace[RASTER_SIZE][RASTER_SIZE] == NO_DATA:
        displace[RASTER_SIZE][RASTER_SIZE] = 500 * randf(-1, 1)
    fractal(displace, (0, 0), (RASTER_SIZE, RASTER_SIZE), 250, randf)
    maxi = -10000
    mini = 10000
    for i in range(0, RASTER_SIZE):
        for j in range(0, RASTER_SIZE):
            mini = min(displace[i][j], mini)
            maxi = max(displace[i][j], maxi)
    return displace

def seed(pts):
    """Generate a seed for an array that varies on last level."""
    rseed = 0
    for i in pts:
        rseed = 1000 * rseed + i
    return rseed

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
    """Maximum difference of existing array indices."""
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

def local_create(rasters, lb_a, ru_a, pt_ad):
    """Iterate over all heights and create a local raster elevation field"""
    ptl_lon1 = int(lb_a[4]) if pt_ad[2] == int(lb_a[2]) else 0
    ptl_lat1 = int(lb_a[5]) if pt_ad[3] == int(lb_a[3]) else 0
    ptl_lon2 = int(ru_a[4]) if pt_ad[2] == int(ru_a[2]) else 0
    ptl_lat2 = int(ru_a[5]) if pt_ad[3] == int(ru_a[3]) else 0
    for ptl_lon in range(ptl_lon1, ptl_lon2 + 1):
        for ptl_lat in range(ptl_lat1, ptl_lat2 + 1):
            local_frame(rasters, [pt_ad[0], pt_ad[1], pt_ad[2], pt_ad[3], ptl_lon, ptl_lat])

def local_frame(rasters, pts):
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
    displace = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)

    # Prepare displace from already completed squares
    left = read_geotiff(neighbor_id(Dir.LEFT, pts[0:6]), 2)
    if left is not None:
        displace[:, 0] = left[:, RASTER_SIZE]
    right = read_geotiff(neighbor_id(Dir.RIGHT, pts[0:6]), 2)
    if right is not None:
        displace[:, RASTER_SIZE] = right[:, 0]
    down = read_geotiff(neighbor_id(Dir.DOWN, pts[0:6]), 2)
    if down is not None:
        displace[0, :] = down[RASTER_SIZE, :]
    top = read_geotiff(neighbor_id(Dir.TOP, pts[0:6]), 2)
    if top is not None:
        displace[RASTER_SIZE, :] = top[0, :]

    LOGGER.debug("left %s", displace[0, :])
    LOGGER.debug("right %s", displace[RASTER_SIZE, :])
    LOGGER.debug("down %s", displace[:, 0])
    LOGGER.debug("top %s", displace[:, RASTER_SIZE])

    # Fill remainder
    displace = init_displace(displace, rseed=seed(pts))
    LOGGER.debug("random %s", displace)

    domain_id = ".".join([str(p) for p in pts[0:4]])
    domain_field = read_geotiff(domain_id, 1)
    field = local_calc(domain_field, pts, displace)
    write_sql(rasters[2], (field, displace), local_id, geo_pts, L2D * D2A)
    write_geotiff((field, displace), local_id, geo_pts, L2D * D2A)

def local_calc(domain_field, pts, displace):
    """Calculate the domain height field."""
    LOGGER.info(
        "Make local raster starting at [%s,%s,%s,%s,%s,%s]",
        pts[0], pts[1], pts[2], pts[3], pts[4], pts[5]
    )
    field = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)
    geo_x = pts[0] + pts[2] / D2A + pts[4] / L2D / D2A
    ras_x = 0
    while ras_x < RASTER_SIZE + 1:
        LOGGER.debug("longitude at %s", geo_x)
        ras_dx = int((geo_x - pts[0] - pts[2] / D2A) * D2A * RASTER_SIZE)
        frac_lx = (geo_x - pts[0] - pts[2] / D2A) * D2A * RASTER_SIZE - ras_dx
        geo_y = pts[1] + pts[3] / D2A + pts[5] / L2D / D2A
        ras_y = 0
        while ras_y < RASTER_SIZE + 1:
            ras_dy = int((geo_y - pts[1] - pts[3] / D2A) * D2A * RASTER_SIZE)
            frac_ly = (geo_y - pts[1] - pts[3] / D2A) * D2A * RASTER_SIZE - ras_dy
            fract_h = displace[ras_y][ras_x] / 1000
            avg = weighted_average(domain_field, (ras_dy, ras_dx), (1 - frac_ly, 1 - frac_lx))
            field[ras_y][ras_x] = avg + fract_h * LOCAL_DEV_H
            geo_y += 1 / RASTER_SIZE / L2D / D2A
            ras_y += 1
        geo_x += 1 / RASTER_SIZE / L2D / D2A
        ras_x += 1
    return field

def domain_create(rasters, lb_a, ru_a, pt_a):
    """Iterate over all heights and create a domain raster elevation field"""
    ptd_lon1 = int(lb_a[2]) if pt_a[0] == int(lb_a[0]) else 0
    ptd_lat1 = int(lb_a[3]) if pt_a[1] == int(lb_a[1]) else 0
    ptd_lon2 = int(ru_a[2]) if pt_a[0] == int(ru_a[0]) else 0
    ptd_lat2 = int(ru_a[3]) if pt_a[1] == int(ru_a[1]) else 0
    for ptd_lon in range(ptd_lon1, ptd_lon2 + 1):
        for ptd_lat in range(ptd_lat1, ptd_lat2 + 1):
            domain_frame(rasters, [pt_a[0], pt_a[1], ptd_lon, ptd_lat])
            if len(lb_a) > 4:
                local_create(rasters, lb_a, ru_a, (pt_a[0], pt_a[1], ptd_lon, ptd_lat))

def domain_frame(rasters, pts):
    """Calculate the domain-level height field."""
    domain_id = ".".join([str(p) for p in pts[0:4]])
    geo_pts = (pts[0] + pts[2] / D2A, pts[1] + pts[3] / D2A)
    if read(rasters[1], domain_id, geo_pts, D2A):
        LOGGER.info("Found %s", domain_id)
        return
    LOGGER.info("Calculate %s", domain_id)
    displace = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)

    # Prepare displace from already completed squares
    left = read_geotiff(neighbor_id(Dir.LEFT, pts[0:4]), 2)
    if left is not None:
        displace[:, 0] = left[:, RASTER_SIZE]
    right = read_geotiff(neighbor_id(Dir.RIGHT, pts[0:4]), 2)
    if right is not None:
        displace[0, RASTER_SIZE] = right[:, 0]
    down = read_geotiff(neighbor_id(Dir.DOWN, pts[0:4]), 2)
    if down is not None:
        displace[0, :] = down[RASTER_SIZE, :]
    top = read_geotiff(neighbor_id(Dir.TOP, pts[0:4]), 2)
    if top is not None:
        displace[RASTER_SIZE, :] = top[0, :]

    LOGGER.debug("left %s", displace[0, :])
    LOGGER.debug("right %s", displace[RASTER_SIZE, :])
    LOGGER.debug("down %s", displace[:, 0])
    LOGGER.debug("top %s", displace[:, RASTER_SIZE])

    # Fill remainder
    displace = init_displace(displace, rseed=seed(pts))
    LOGGER.debug("random %s", displace)

    atlas_id = ".".join([str(p) for p in pts[0:2]])
    atlas_field = read_geotiff(atlas_id, 1)
    field = domain_calc(atlas_field, pts, displace)
    write_sql(rasters[1], (field, displace), domain_id, geo_pts, D2A)
    write_geotiff((field, displace), domain_id, geo_pts, D2A)

def domain_calc(atlas_field, pts, displace):
    """Calculate the domain height field."""
    LOGGER.info("Make domain raster for [%s,%s,%s,%s]", pts[0], pts[1], pts[2], pts[3])
    field = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)
    geo_x = pts[0] + pts[2] / D2A
    ras_x = 0
    while ras_x < RASTER_SIZE + 1:
        LOGGER.debug("longitude at %s", geo_x)
        ras_ax = int((geo_x - pts[0]) * RASTER_SIZE)
        frac_dx = (geo_x - pts[0]) * RASTER_SIZE - ras_ax
        geo_y = pts[1] + pts[3] / D2A
        ras_y = 0
        while ras_y < RASTER_SIZE + 1:
            LOGGER.debug("latitude at %s", geo_y)
            ras_ay = int((geo_y - pts[1]) * RASTER_SIZE)
            frac_dy = (geo_y - pts[1]) * RASTER_SIZE - ras_ay
            fract_h = displace[ras_y][ras_x] / 1000
            avg = weighted_average(atlas_field, (ras_ay, ras_ax), (1 - frac_dy, 1 - frac_dx))
            field[ras_y][ras_x] = avg + fract_h * DOMAIN_DEV_H
            geo_y += 1 / RASTER_SIZE / D2A
            ras_y += 1
        geo_x += 1 / RASTER_SIZE / D2A
        ras_x += 1
    return field

def atlas_create(polygons, points, rasters, lb_a, ru_a):
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
            atlas_frame(rasters, polygons, points, (pta_lon, pta_lat))
            if len(lb_a) > 2:
                domain_create(rasters, lb_a, ru_a, (pta_lon, pta_lat))

def atlas_frame(rasters, polygons, points, pts):
    """Calculate the atlas-level height field."""
    atlas_id = ".".join([str(p) for p in pts[0:2]])
    geo_pts = (pts[0], pts[1])
    if read(rasters[0], atlas_id, geo_pts, 1):
        LOGGER.info("Found %s", atlas_id)
        return
    LOGGER.info("Calculate %s", atlas_id)
    displace = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)

    # Prepare displace from already completed squares
    left = read_geotiff(neighbor_id(Dir.LEFT, pts[0:2]), 2)
    if left is not None:
        displace[:, 0] = left[:, RASTER_SIZE]
    right = read_geotiff(neighbor_id(Dir.RIGHT, pts[0:2]), 2)
    if right is not None:
        displace[:, RASTER_SIZE] = right[:, 0]
    down = read_geotiff(neighbor_id(Dir.DOWN, pts[0:2]), 2)
    if down is not None:
        displace[0, :] = down[RASTER_SIZE, :]
    top = read_geotiff(neighbor_id(Dir.TOP, pts[0:2]), 2)
    if top is not None:
        displace[RASTER_SIZE, :] = top[0, :]

    LOGGER.debug("left %s", displace[0, :])
    LOGGER.debug("right %s", displace[RASTER_SIZE, :])
    LOGGER.debug("down %s", displace[:, 0])
    LOGGER.debug("top %s", displace[:, RASTER_SIZE])

    # Fill remainder
    displace = init_displace(displace, rseed=seed(pts))
    LOGGER.debug("random %s", displace)

    field = atlas_calc(polygons, points, pts, displace)
    write_sql(rasters[0], (field, displace), atlas_id, geo_pts, 1)
    write_geotiff((field, displace), atlas_id, geo_pts, 1)

def atlas_calc(polygons, points, pts, displace):
    """Calculate the atlas height field."""
    partitions = get_partitions(polygons, points, [pts[0], pts[1], pts[0] + 1, pts[1] + 1])
    field = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)
    LOGGER.info("Make atlas raster for [%s,%s]", pts[0], pts[1])
    geo_x = pts[0]
    ras_x = 0
    while ras_x < RASTER_SIZE:
        LOGGER.debug("longitude at %s", geo_x)
        geo_y = pts[1]
        ras_y = 0
        while ras_y < RASTER_SIZE:
            fract_h = displace[ras_y][ras_x] / 1000
            avg = handle_partitions(polygons, points, partitions, [geo_x, geo_y])
            field[ras_y][ras_x] = avg + fract_h * ATLAS_DEV_H
            geo_y += 1 / RASTER_SIZE
            ras_y += 1
        geo_x += 1 / RASTER_SIZE
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

    atlas_create(polygons, points, rasters, lb_a, ru_a)

def tests():
    """Tests."""
    seed_tests()
    fractal_tests()
    neighbor_tests1()
    neighbor_tests2()
    neighbor_tests3()
    interpolation_tests1()
    interpolation_tests2()
    interpolation_tests3()
    side_tests1()
    side_tests2()
    calc_bary_test()

def seed_tests():
    """Test different seeds at lowest level."""
    num_tests = 0

    num_tests += 1
    s_1 = seed((-10, -10, 10, 10, 0, 0))
    s_2 = seed((-10, -10, 10, 10, 0, 1))
    s_3 = seed((-10, -10, 10, 10, 0, 2))

    s_5 = seed((-10, -10, 10, 10, 1, 0))
    s_6 = seed((-10, -10, 10, 10, 2, 0))

    assert(s_1 != s_2 and s_2 != s_3 and s_1 != s_3)
    assert(s_1 != s_5 and s_5 != s_6 and s_1 != s_6)
    assert(s_2 != s_5 and s_2 != s_6 and s_3 != s_5 and s_3 != s_6)

    num_tests += 1
    s_1 = seed((-10, -10, 0, 0))
    s_2 = seed((-10, -10, 0, 1))
    s_3 = seed((-10, -10, 0, 2))

    s_5 = seed((-10, -10, 1, 0))
    s_6 = seed((-10, -10, 2, 0))

    assert(s_1 != s_2 and s_2 != s_3 and s_1 != s_3)
    assert(s_1 != s_5 and s_5 != s_6 and s_1 != s_6)
    assert(s_2 != s_5 and s_2 != s_6 and s_3 != s_5 and s_3 != s_6)

    num_tests += 1
    s_1 = seed((0, 0))
    s_2 = seed((0, 1))
    s_3 = seed((0, 2))

    s_5 = seed((1, 0))
    s_6 = seed((2, 0))

    assert(s_1 != s_2 and s_2 != s_3 and s_1 != s_3)
    assert(s_1 != s_5 and s_5 != s_6 and s_1 != s_6)
    assert(s_2 != s_5 and s_2 != s_6 and s_3 != s_5 and s_3 != s_6)

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def fractal_tests():
    """Fractal simple tests."""
    num_tests = 0

    num_tests += 1
    field = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)
    field = init_displace(field, lambda x, y: x)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert (field[i][j] >= -1164.0625 and field[i][j] <= 1164.0625)
    assert any([
        field[i][j] > 1160 or field[i][j] < -1160
        for i in range(0, RASTER_SIZE + 1) for j in range(0, RASTER_SIZE + 1)
    ])

    num_tests += 1
    field = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)
    field = init_displace(field, lambda x, y: y)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert (field[i][j] >= -1164.0625 and field[i][j] <= 1164.0625)
    assert any([
        field[i][j] > 1160 or field[i][j] < -1160
        for i in range(0, RASTER_SIZE + 1) for j in range(0, RASTER_SIZE + 1)
    ])

    num_tests += 1
    field = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), NO_DATA, numpy.float32)
    field = init_displace(field, lambda x, y: 0)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
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
    displace = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # i domain
    num_tests += 1
    atlas = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            atlas[j, i] = i
    domain = domain_calc(atlas, (0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(domain[j][i], i/16)

    # j domain
    num_tests += 1
    atlas = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            atlas[j, i] = j
    domain = domain_calc(atlas, (0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(domain[j][i], j/16)

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def interpolation_tests2():
    """Interpolation on domain and local level."""
    displace = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # i*j domain
    num_tests += 1
    atlas = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            atlas[j, i] = i*j
    domain = domain_calc(atlas, (0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(domain[j][i], i/16 * j/16)

    # i + j local
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            domain[j, i] = j+i
    local = local_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(local[j][i], i/16 + j/16)

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def interpolation_tests3():
    """Interpolation on local level."""
    displace = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # i local
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            domain[j, i] = i
    local = local_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(local[j][i], i/16)

    # j local
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            domain[j, i] = j
    local = local_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(local[j][i], j/16)

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def side_tests1():
    """Neighbor effects."""
    displace = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # left local
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    domain[0, :] = 1
    local = local_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(local[j][i], max(0, 1 - j/16))

    # right local
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    domain[RASTER_SIZE, :] = 1
    local = local_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(local[j][i], max(0, j/16 - RASTER_SIZE + 16))

    #  down local
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    domain[:, 0] = 1
    local = local_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(local[j][i], max(0, 1 - i/16))

    #  top local
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    domain[:, RASTER_SIZE] = 1
    local = local_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(local[j][i], max(0, i/16 - RASTER_SIZE + 16))

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def side_tests2():
    """Neighbor effects."""
    displace = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    num_tests = 0

    # left domain
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    domain[0, :] = 1
    domain = domain_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(domain[j][i], max(0, 1 - j/16))

    # right domain
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    domain[RASTER_SIZE, :] = 1
    domain = domain_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(domain[j][i], max(0, j/16 - RASTER_SIZE + 16))

    #  down domain
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    domain[:, 0] = 1
    domain = domain_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(domain[j][i], max(0, 1 - i/16))

    #  top domain
    num_tests += 1
    domain = numpy.full((RASTER_SIZE + 1, RASTER_SIZE + 1), 0, numpy.float32)
    domain[:, RASTER_SIZE] = 1
    domain = domain_calc(domain, (0, 0, 0, 0, 0, 0), displace)
    for i in range(0, RASTER_SIZE + 1):
        for j in range(0, RASTER_SIZE + 1):
            assert math.isclose(domain[j][i], max(0, i/16 - RASTER_SIZE + 16))

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")

def calc_bary_test():
    """Check that barymetric height yields correct values."""
    num_tests = 0

    num_tests += 1
    assert calc_bary([['1000', 1], ['500', 1]], 0, 0) == 750
    num_tests += 1
    assert calc_bary([['1000', 1], ['500', 1]], 0, 1000) == 750
    assert calc_bary([['1000', 1], ['500', 1]], 0, -1000) == 750
    num_tests += 1
    assert calc_bary([['1000', 2], ['500', 2]], 0, 1000) == 750
    assert calc_bary([['1000', 2], ['500', 2]], 0, -1000) == 750
    num_tests += 1
    assert calc_bary([['1000', 1]], 2, 100) == 1000 + 100/3
    assert calc_bary([['1000', 1]], 2, -100) == 1000 - 100/3
    num_tests += 1
    assert calc_bary([['1000', 1]], 1, 1000) == 1500
    assert calc_bary([['1000', 1]], 1, -1000) == 500
    num_tests += 1
    assert calc_bary([['1000', 1]], 2, 1000) == 1000 + 1000/3
    assert calc_bary([['1000', 1]], 2, -1000) == 1000 - 1000/3

    print(f"> {inspect.stack()[0][3]}: {num_tests} tests passed")
