#!/bin/sh

# To run this Makefile, your environment must provide: docker,
# ogr2ogr, python Edit the svg definition below to point to
# HarnAtlas-Clean-01.74.svg Also think about the db connection string
# in the python files. Unfortunately they are not centralized yet.
# After the Makefile runs, the map should be visible at
# http://localhost

svg = ~/Downloads/HarnAtlas-Clean-01.91.svg
db = PG:"dbname=dbname host=localhost user=user port=25432 password=password"

start: setup ah

postgis:
	docker compose up --detach --wait

ah: ah_lines.json ah_polygons.json ah_points.json

ah_lines.json:
	ogr2ogr -f GeoJSON -s_srs kethira-sin30w.wkt -t_srs kethira-sphere.wkt ah_lines.json $(db) raw_lines

ah_polygons.json:
	ogr2ogr -f GeoJSON -s_srs kethira-sin30w.wkt -t_srs kethira-sphere.wkt ah_polygons.json $(db) raw_polygons

ah_points.json:
	ogr2ogr -f GeoJSON -s_srs kethira-sin30w.wkt -t_srs kethira-sphere.wkt ah_points.json $(db) raw_points

geo_lines.json geo_polygons.json geo_points.json geo.svg:
	python svg2geo.py -i $(svg) -o geo

setup: postgis
	docker exec harn-atlas-tools-db-1 psql postgresql://user:password@localhost:5432/dbname\
	 -c "DROP TABLE IF EXISTS raw_lines"\
	 -c "DROP TABLE IF EXISTS raw_polygons"\
	 -c "DROP TABLE IF EXISTS raw_points"\
	 -c "CREATE EXTENSION IF NOT EXISTS postgis_sfcgal"\
	 -c "CREATE EXTENSION IF NOT EXISTS postgis_raster"
	python geo2raw -i geo -o raw
	python raw2ext -i raw -e contours,names,coast,lakes,roads,flora,rivers,raster

clean:
	rm -f geo_lines.json geo_polys.json geo_pts.json

clobber: clean
	rm -f ah_lines.json ah_polys.json ah_pts.json
