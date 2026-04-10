# harn-atlas-tools

A script collection to extract GIS data from Harn Atlas Map exports.
All scripts take the -v flag.  Scripts that do not completely digest
all entries usually print the number of remaining lines at the end.
They will be considered in a later step or, if that proves impossible,
they need to be eyeballed.

Runtime is an estimate on my PC.

The scripts are not re-entrant, i.e. don't call them a second time on
the modified dataset.  Many scripts take a -T as option to execute
some tests.  All take a -v option to output more debug information.

## Extraction

The following is the rough procedure to follow.  First extract *name*
information with

    python svg_replace.py -i ~/Downloads/HarnAtlas-Clean-01.91.svg -o ~/transformed.svg

This also exchanges graphical letters with real text, which probably
don't render nice but will be put into the database for later
analysis.

> Runtime: 20 seconds

Then

    python svg2geo.py -i ~/transformed.svg -o xyz.json

This will create points, polygons and lines in separate files, called
`xyz_<type>.json`, respectively. Because Shape files cannot have
different geometries in one file, they are separated.  Only Shape
files and GeoJson are supported.  Of course, `xyz` can be exchanged
with any other name (avoid blanks).

Make sure you have a postgres database set up, so the following will
work.  Include the postgis extensions.  I had a bug in the geometry
containment section, but that automatically got resolved after a
fruitless try of an upgrade and a reboot of my machine.  Just make
sure your versions are fairly recent.

The script now also evaluates style information to be considered in
heuristics later.

> Runtime: 1 minute

## DB preparation

    ogr2ogr -f PostgreSQL PG:"dbname=dbname host=localhost user=user port=5432 password=password" xyz_lines.json -nln xyz_lines
    ogr2ogr -f PostgreSQL PG:"dbname=dbname host=localhost user=user port=5432 password=password" xyz_pts.json -nln xyz_pts
    ogr2ogr -f PostgreSQL PG:"dbname=dbname host=localhost user=user port=5432 password=password" xyz_polys.json -nln xyz_polys

Replace `dbname`, `user`, `password`, `xyz` with whatever makes sense
for you. This will dump the lines into the table `xyz_lines`. After
this, we can make use of the index on the geo-coordinates.

You can also use ogr2ogr to convert db data into Shapefiles and
GeoJson or a lot of other things. A great tool from a great toolset.

> Runtime: 1 minute total

For the current export, execute the following SQL statement on your DB.

    psql postgresql://user:password@localhost:port/dbname -f dummies.sql

to *xyz_lines*.  This yields a (fake) closed coastline and benefits
coast and river calculations.  The accuracy of these additional
insertions is proven for *EPS = 0.006* in the various scripts.
Changing EPS may make these lines inaccurate.

## Extract Names

This works surprisingly well.

    python geo_pts.py -t xyz -d user:password@dbname:localhost:port

> Runtime: 2 minutes

## Elevation

    python geo_elevation.py -t xyz -d user:password@dbname:host:port

This step extracts the elevation lines and assigns height labels to
the based on the following heuristics:

* Any label satisfying the regex `\[\^1-9\]\(\[1-9\]\[05\]\|5\)00` is a height label.
  If you are into this, don't copy this from markdown.

* Remove one erroneous line.

* the largest number of close (*EPSP*) labels wins.

* connect all endpoints of lines within *EPSL*.

* All unlabeled rings around peaks go in 500ft steps to the outermost
  labeled ring.

* Lines closed will be turned into polygons.

The type field in the table contains the elevation.  About 200 lines
have no label at this point.  This heuristic improves with the number
of closed elevation lines.  With Harn being an island this will
eventually decrease when all lines will be closed.

> Runtime: 5 minutes

## Coast line

Effectively, this is the 0 elevation line and this is how it will be
treated in later steps.

    python geo_coast.py -t xyz -d user:password@dbname:host:port

will detect all closed coastlines (including inland islands) and
remove rivers by a simple heuristic.  The coasts are not considered by
`geo_elevation.py` yet. This will also find the big lakes that are
connected to the coastline; Arain & Tontury currently.

* Uses *EPSL* to bridge shore gaps and *EPSB* to squeeze out rivers.

> Runtime: 2 minutes

## Lakes

This determines all lakes by looking at the fill color.  Elevation of
lakes is not created, calculations are too complex at this point.  In
particular, some have elevation in (currently not recovered) text.

    python geo_lakes.py -t xyz -d user:password@dbname:host:port

* Ignore pathological lakes smaller than *EPS*.

> Runtime: seconds

## Roads

This extracts roads as they were intended from the SVG.  At this point
the runtime is less than a minute.

    python geo_roads.py -t xyz -d user:password@dbname:host:port

will connect towns (and such) and roads by modifying the lines and
tables.  The final roads appear as *type = 'Trail|Unpaved|Paved'*, but
the original is also modified.

* connect all road end-points within distance *EPSG* to the road
  network. and all road points to locations within the same distance.

* Remove short end artifacts from the road network.

> Runtime: 1 minute

## Vegetation

Turns the WOODLAND, CROPLAND, HEATH, FOREST, NEEDLELEAF, ALPINE,
SNOW_x2F_ICE into multipolygons (in the postgis sense).

    python geo_vegetation.py -t xyz -d user:password@dbname:host:port

Any set at position *n* in this list is reduced by every multipolygon
at later positions.  I.e. the multipolygons are disjoint.  position
*0* is going to be the default, filling all land area not filled
otherwise.

* The above is called "reduce & normalize" in the script.

* Use *EPS* to grow a bit to cover draw glitches.

* Vegetation is restricted to land.

* Shoal/Reef is restricted to off land.

* The results are in the *xyz_polys* table, type prefixed with `VEG/`.

> Runtime: 3 minutes

## Rivers

Determines rivers from shores to springs in iterations.  Rivers are
created with type *River/n/Mouth:vertex*, where *n* is the level (from
0 at the coast) of detection and *vertex* is *start* or *end*,
depending on the orientation of the linestring.

    python geo_rivers.py -t xyz -d user:password@dbname:host:port

> Runtime: 45 minutes

## Elevation field

The next step is a a computationally more involved process.
Interpolation is not trivial and needs some *numerical stability*.

    python geo_height.py -v -t xyz -g -16 40 -H 32 -d user:password@dbname:localhost:25432

The script creates an elevation field called *all.tif*.  The *--geo*
option takes *lon* and *lat* coordinates, the above example is
arbitrary, but relevant for the time the script takes. The *--hscale*
option maps the map elevations given in feet to the out band and
should logically by be about 1/3.  I found 32 to be useful for
debugging.  This version dumps all elevation raster fields into the
table *..._heights* of the db.

Currently a scale of 400m x 400m pixels is hard-coded, this takes a
couple of minutes on Melderyn.  Note that the geo coordinates take
only integer degrees and a run covers only a 1x1 "degree square".  The
runtime heavily depends on the density of elevation features in it, so
no meaningful runtime estimate can be given.

> Runtime: a few minutes
