#!/usr/bin/python
"""
Pipe the geo json files into the raw db.  Includes some dummies, which
depend on the version of the file you import.
"""
import subprocess
import argparse
import logging
import sys
import geo2raw

LOGGER = logging.getLogger(__name__)

def main():
    """Main (wrapper) method."""

    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Convert Harn GeoJSONs to raw SQL.'
    )
    parser.add_argument(
        '-i', '--inpre', dest='inpre', help='input files\' prefix', required=True
    )
    parser.add_argument(
        '-o', '--outpre', dest='outpre', help='output tables\' prefix', required=True
    )
    parser.add_argument(
        '-l', '--log-level', default=logging.INFO, type=lambda x: getattr(logging, x),
        help='configure log level', required=False
    )
    parser.add_argument(
        '-T', '--test', action='store_true', help='run tests instead', required=False
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    if args.test:
        pass
    else:
        cmd = ['ogr2ogr', '-f', 'PostgreSQL', geo2raw.DBC1]
        ipre = f"{args.inpre}_"
        opre = f"{args.outpre}_"
        LOGGER.info("Convert lines...")
        subprocess.run(
            cmd +  [f'{ipre}lines.json', '-nln', f'{opre}lines'], check=True
        )
        LOGGER.info("Convert points...")
        subprocess.run(
            cmd + [f'{ipre}points.json', '-nln', f'{opre}points'], check=True
        )
        LOGGER.info("Convert polygons...")
        subprocess.run(
            cmd + [f'{ipre}polygons.json', '-nln', f'{opre}polygons'], check=True
        )
        LOGGER.info("Add dummies...")
        dummies = open('geo2raw/dummies.sql', 'r')
        for dummy in dummies:
            dummy = dummy.replace('raw_lines', f'{opre}lines')
            subprocess.run(['psql', geo2raw.DBC2, '-c', dummy], check=False)

        srid0 = "UpdateGeometrySRID"
        subprocess.run(
            ['psql', geo2raw.DBC2, '-c', f"SELECT {srid0}('{opre}lines','wkb_geometry', 0)"],
            check=True
        )
        subprocess.run(
            ['psql', geo2raw.DBC2, '-c', f"SELECT {srid0}('{opre}points','wkb_geometry', 0)"],
            check=True
        )
        subprocess.run(
            ['psql', geo2raw.DBC2, '-c', f"SELECT {srid0}('{opre}polygons','wkb_geometry', 0)"],
            check=True
        )

if __name__ == '__main__':
    main()
