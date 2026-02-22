#!/bin/sh

# To run this Makefile, your environment must provide: docker, ogr2ogr, python
# Edit the svg definition below to point to HarnAtlas-Clean-01.74.svg
# After the Makefile runs, the map should be visible at http://localhost

svg = ~/Downloads/HarnAtlas-Clean-01.74.svg
db = PG:"dbname=dbname host=localhost user=user port=25432 password=password"
creds = user:password@dbname:localhost:25432


start: setup ah

postgis:
	docker compose up --detach --wait

ah: ah_lines.json ah_polys.json ah_pts.json

ah_lines.json:
	ogr2ogr -f GeoJSON -s_srs kethira-sin30w.wkt -t_srs kethira-sphere.wkt ah_lines.json $(db) xyz_lines

ah_polys.json:
	ogr2ogr -f GeoJSON -s_srs kethira-sin30w.wkt -t_srs kethira-sphere.wkt ah_polys.json $(db) xyz_polys

ah_pts.json:
	ogr2ogr -f GeoJSON -s_srs kethira-sin30w.wkt -t_srs kethira-sphere.wkt ah_pts.json $(db) xyz_pts

xyz_lines.json xyz_polys.json xyz_pts.json:
	python svg2geo.py -i $(svg) -o xyz.json

setup: postgis xyz_lines.json xyz_polys.json xyz_pts.json
	docker exec harn-atlas-tools-db-1 psql postgresql://user:password@localhost:5432/dbname\
	 -c "DROP TABLE IF EXISTS xyz_lines"\
	 -c "DROP TABLE IF EXISTS xyz_polys"\
	 -c "DROP TABLE IF EXISTS xyz_pts"\
         -c "CREATE EXTENSION IF NOT EXISTS postgis_sfcgal"
         -c "CREATE EXTENSION IF NOT EXISTS postgis_raster"
	ogr2ogr -f PostgreSQL $(db) xyz_lines.json -nln xyz_lines
	ogr2ogr -f PostgreSQL $(db) xyz_polys.json -nln xyz_polys
	ogr2ogr -f PostgreSQL $(db) xyz_pts.json -nln xyz_pts
	docker exec harn-atlas-tools-db-1 psql postgresql://user:password@localhost:5432/dbname\
	 -c "SELECT UpdateGeometrySRID('xyz_lines','wkb_geometry',0)"\
	 -c "SELECT UpdateGeometrySRID('xyz_polys','wkb_geometry',0)"\
	 -c "SELECT UpdateGeometrySRID('xyz_pts','wkb_geometry',0)"
	python geo_elevation.py -t xyz -d $(creds)
	python geo_coast.py -t xyz -d $(creds)
	python geo_lakes.py -t xyz -d $(creds)
	python geo_roads.py -t xyz -d $(creds)
	python geo_vegetation.py -t xyz -d $(creds)
	python geo_rivers.py -t xyz -d $(creds)

clean:
	rm -f xyz_lines.json xyz_polys.json xyz_pts.json

clobber: clean
	rm -f ah_lines.json ah_polys.json ah_pts.json
