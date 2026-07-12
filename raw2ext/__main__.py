#!/usr/bin/python
"""
Convert a 'Harn Atlas Map' SVG into GeoJSON format.  Some details are
caused by specific idiosyncracies of such maps.  Also read the help.
"""
import logging
import argparse
import sys
import raw2ext.names
import raw2ext.contours
import raw2ext.coast
import raw2ext.lakes
import raw2ext.roads
import raw2ext.flora
import raw2ext.rivers

LOGGER = logging.getLogger(__name__)

def tests(extract, inpre):
    """Test collector."""

    if 'contours' in extract:
        raw2ext.contours.tests(inpre)
    if 'coast' in extract:
        raw2ext.coast.tests(inpre)
    if 'rivers' in extract:
        raw2ext.rivers.tests(inpre)

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
        '-e', '--extract', dest='extract', help='extraction list ', required=True
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

    extract = args.extract.split(",")
    if args.test:
        tests(extract, args.inpre)
    else:
        if 'contours' in extract:
            raw2ext.contours.main(args.inpre)
            raw2ext.CONNECT.commit()
        if 'names' in extract:
            raw2ext.names.main(args.inpre)
            raw2ext.CONNECT.commit()
        if 'coast' in extract:
            raw2ext.coast.main(args.inpre)
            raw2ext.CONNECT.commit()
        if 'lakes' in extract:
            raw2ext.lakes.main(args.inpre)
            raw2ext.CONNECT.commit()
        if 'roads' in extract:
            raw2ext.roads.main(args.inpre)
            raw2ext.CONNECT.commit()
        if 'flora' in extract:
            raw2ext.flora.main(args.inpre)
            raw2ext.CONNECT.commit()
        if 'rivers' in extract:
            raw2ext.rivers.main(args.inpre)
            raw2ext.CONNECT.commit()

if __name__ == '__main__':
    main()
