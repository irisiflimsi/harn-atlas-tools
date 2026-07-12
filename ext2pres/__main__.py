#!/usr/bin/python
"""
Convert a 'Harn Atlas Map' SVG into GeoJSON format.  Some details are
caused by specific idiosyncracies of such maps.  Also read the help.
"""
import logging
import argparse
import sys
import ext2pres.raster

LOGGER = logging.getLogger(__name__)

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description=
        '''
        Convert Harn SVG to GeoJSON. The extraction list is comma-separated
        and contains (without quotes) 'names', 'contours'
        '''
    )
    parser.add_argument(
        '-i', '--inpre', dest='inpre', help='input tables\' prefix', required=True
    )
    parser.add_argument(
        '-o', '--outpre', dest='outpre', help='output tables\' prefix', required=True
    )
    parser.add_argument(
        '-l', '--log-level', dest='loglevel', default=logging.INFO,
        type=lambda x: getattr(logging, x), help='configure log level'
    )
    parser.add_argument(
        '-T', '--test', action='store_true', help='run tests instead'
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=args.loglevel, format='%(relativeCreated).0f %(name)s:%(lineno)d %(message)s'
    )

    inpre = args.inpre
    outpre = args.outpre

    # Initialize
    ext2pres.sql(f"""
      CREATE TABLE IF NOT EXISTS {outpre}_world_rasters (id integer primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_world_rasters_idx ON {outpre}_world_rasters
        USING GIST(ST_Envelope(raster));
      CREATE TABLE IF NOT EXISTS {outpre}_region_rasters (id integer primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_region_rasters_idx ON {outpre}_region_rasters
        USING GIST(ST_Envelope(raster));
      CREATE TABLE IF NOT EXISTS {outpre}_atlas_rasters (id integer primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_atlas_rasters_idx ON {outpre}_atlas_rasters
        USING GIST(ST_Envelope(raster));
      CREATE TABLE IF NOT EXISTS {outpre}_domain_rasters (id integer primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_domain_rasters_idx ON {outpre}_domain_rasters
        USING GIST(ST_Envelope(raster));
      CREATE TABLE IF NOT EXISTS {outpre}_local_rasters (id integer primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_local_rasters_idx ON {outpre}_local_rasters
        USING GIST(ST_Envelope(raster));
    """)

    ext2pres.raster.main(inpre, outpre)
    ext2pres.CONNECT.commit()

if __name__ == '__main__':
    main()
