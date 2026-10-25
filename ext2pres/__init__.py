#!/usr/bin/python
"""
Constructor artifact. See __main__.py for package docs.
"""
import os
import logging
import numpy
import rasterio
import psycopg2

# Database connection
DBC = "user:password@dbname:localhost:25432"

# 1 degree is roughly 100km.  For fractals we need powers of two.
# Raster size is 1 / width of raster pixel in degrees.
RASTER_SIZE = 256 # 100/256 km pixels

# How many local pixels per domain pixel (squared)
L2D = 16 # Local : Domain details

# How many domain pixels per atlas pixel (squared)
D2A = 16 # Domain : Atlas details

# Treated as GeoTIF default value
NO_DATA = 32767 # GeoTIF default

CONNECT = psycopg2.connect(
    user=f"{DBC.split('@')[0].split(':')[0]}",
    password=f"{DBC.split('@')[0].split(':')[1]}",
    database=f"{DBC.split('@')[1].split(':')[0]}",
    host=f"{DBC.split('@')[1].split(':')[1]}",
    port=f"{DBC.split('@')[1].split(':')[2]}"
)

CURSOR = CONNECT.cursor()

LOGGER = logging.getLogger(__name__)

def sql(*args):
    """Shorthand for execute and fetchall."""
    if len(args) > 0:
        CURSOR.execute(args[0])
        return None
    return CURSOR.fetchall()

def calc_bary(hdpts, diam, deviation):
    """Calculate barycentric elevation in feet."""
    hsum = 0
    dsum = 0
    if diam > 0:
        # There is only the mesa boundary with diameter diam.  The
        # addition is bound by (1/2)/(1 + 1/2) = 1/3
        hsum = float(hdpts[0][0]) + 3*deviation*(hdpts[0][1]/diam) / (1 + hdpts[0][1]/diam)
    else:
        # SUM_i (hi/di) / SUM_j (1/dj)
        for hdj in hdpts:
            if float(hdj[1]) != 0:
                dsum += 1/float(hdj[1])
        for hdi in hdpts:
            if float(hdi[1]) != 0:
                hsum += float(hdi[0]) / float(hdi[1]) / dsum
            else:
                hsum = float(hdi[0])
                break
    return hsum

def read_geotiff(raster_id, band):
    """Read the raster from GeoTIFF."""
    LOGGER.debug("Enter read %s band %s", f"height{raster_id}.tif", band)
    if not os.path.isfile(f"height{raster_id}.tif"):
        return None
    patch = rasterio.open(f"height{raster_id}.tif", mode="r")
    return numpy.asarray(patch.read(band))

def write_geotiff(fields, raster_id, geo_pts, geo_scale):
    """Write the raster into a GeoTIFF."""
    LOGGER.debug("Enter write %s pts=%s scale=%s", f"height{raster_id}.tif", geo_pts, geo_scale)
    real_geo_scale = geo_scale * RASTER_SIZE
    patch = rasterio.open(
        f"height{raster_id}.tif", height=RASTER_SIZE + 1, width=RASTER_SIZE + 1,
        driver="GTiff", count=2, nodata=NO_DATA, dtype='int16', crs=None, mode="w",
        transform=rasterio.transform.Affine(
            1/real_geo_scale, 0, geo_pts[0], 0, 1/real_geo_scale, geo_pts[1], 0, 0, 1
        )
    )
    fields[0][fields[0] is None] = NO_DATA
    fields[1][fields[1] is None] = NO_DATA
    patch.write(fields[0], 1)
    patch.write(fields[1], 2)

def write_sql(raster, fields, raster_id, geo_pts, geo_scale):
    """Write the raster to Postgis."""
    fields[0][fields[0] is None] = NO_DATA
    fields[1][fields[1] is None] = NO_DATA
    real_geo_scale = geo_scale * RASTER_SIZE
    sql(f"""
      INSERT INTO {raster} (id, raster)
      SELECT '{raster_id}',
        ST_SetValues(
          ST_SetValues(
            ST_AddBand(
              ST_AddBand(
                ST_MakeEmptyRaster(
                  {RASTER_SIZE + 1}, {RASTER_SIZE + 1},
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
      ON CONFLICT (id) DO UPDATE SET raster = EXCLUDED.raster
    """)

def read_sql(raster, raster_id, band):
    """Read the raster from Postgis."""
    sql(f"""
      SELECT ST_DumpValues(raster, {band})
      FROM {raster} WHERE id = '{raster_id}'
    """)
    result = sql()
    if len(result) < 1:
        return None
    return numpy.asarray(result[0][0])
