#!/usr/bin/python
"""
Convert a 'Harn Atlas Map' SVG into GeoJSON format.  Some details are
caused by specific idiosyncracies of such maps.  Also read the help.
"""
import logging
import argparse
import re
import sys
import ext2pres.raster

LOGGER = logging.getLogger(__name__)

NUM = r'[-]?[0-9][0-9]?[0-9]?'
HEX = r'[0-9a-f][0-9a-f]'

def match_loc(test):
    """Parse one geo coordinate."""
    match = re.search(rf'({NUM})\.({NUM})\.({NUM})\.({NUM})\.({NUM})\.({NUM})', test)
    if match:
        return [int(match.group(1)), int(match.group(2)), match.group(3), match.group(4), # local
                match.group(5), match.group(6)]
    match = re.search(rf'({NUM})\.({NUM})\.({NUM})\.({NUM})', test)
    if match:
        return [int(match.group(1)), int(match.group(2)), match.group(3), match.group(4)] # domain
    match = re.search(rf'({NUM})\.({NUM})', test)
    if match:
        return [int(match.group(1)), int(match.group(2))] # atlas
    return None

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
        '-A', '--area', dest='area', help='area to create, see README', required=True
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

    if args.test:
        ext2pres.raster.tests()
        return

    (lb_s, ru_s) = args.area.split(",")
    lb_a = match_loc(lb_s)
    ru_a = match_loc(ru_s)
    if lb_a is None or ru_a is None:
        print("cannot parse area arguments")
        sys.exit(1)
    if len(lb_a) != len(ru_a) or len(lb_a) > 6:
        print("area arguments inconsistent")
        sys.exit(1)
    if lb_a[0] > ru_a[0] or lb_a[1] > ru_a[1]:
        print("incorrectly ordered area arguments ")
        sys.exit(1)

    inpre = args.inpre
    outpre = args.outpre

    # Initialize
    ext2pres.sql(f"""
      CREATE TABLE IF NOT EXISTS {outpre}_world_rasters (id text primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_world_rasters_idx ON {outpre}_world_rasters
        USING GIST(ST_Envelope(raster));
      CREATE TABLE IF NOT EXISTS {outpre}_region_rasters (id text primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_region_rasters_idx ON {outpre}_region_rasters
        USING GIST(ST_Envelope(raster));
      CREATE TABLE IF NOT EXISTS {outpre}_atlas_rasters (id text primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_atlas_rasters_idx ON {outpre}_atlas_rasters
        USING GIST(ST_Envelope(raster));
      CREATE TABLE IF NOT EXISTS {outpre}_domain_rasters (id text primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_domain_rasters_idx ON {outpre}_domain_rasters
        USING GIST(ST_Envelope(raster));
      CREATE TABLE IF NOT EXISTS {outpre}_local_rasters (id text primary key, raster raster);
      CREATE INDEX IF NOT EXISTS {outpre}_local_rasters_idx ON {outpre}_local_rasters
        USING GIST(ST_Envelope(raster));
    """)

    ext2pres.raster.main(inpre, outpre, lb_a, ru_a)
    ext2pres.CONNECT.commit()

if __name__ == '__main__':
    main()
