# harn-atlas-tools

The repo contains a script collection to extract GIS data from Harn
Atlas Map exports.  It also contains a presentation suite to present
the results.  The latter contains some personal choices (also see the
architectire) and is far from ready, while the extracting scripts have
already progressed substantially.

While the presentation layer is self-installing (you have to have
*docker* installed, though) the extractions are python scripts, which
require you to potentially install a few dependency packages.

## Transformation

The scripts are not re-entrant, i.e. don't call them a second time.
This may lead to unexpected results but is usually harmless.  They
also modify the data sets, except the original SVG, which remains
unmodified.

The scripts are organized in four groups: *svg2geo*, *geo2raw*,
*raw2ext*, and *ext2pres*.  All take a *-T* switch for testing, even
if no tests are available.  The log-level may be specified by *-l* and
*-h* shows help.

The complete extraction process may take some time, with the current
export from AI called **HarnAtlas-Clean-01.91.svg** it takes about
10-30 minutes. The *Presentation* section contains a complete build,
if you do not care about individual steps.

### svg2geo

This package exchanges many graphical letters with real text, which
probably don't render nice but will be put into the database for later
analysis.  This is done in a preprocessing script.

The main purpose is to create *points*, *polygons*, and *lines* files,
called `geo_<type>.json`, respectively.  They contain *GeoJSON* data.
Additionally, a transformed file *geo.svg* is provided for debugging
purposes.  (The last is irrespective of the logging level.)

The script also evaluates style information to be considered in
heuristics later.

    python svg2geo -i ~/HarnAtlas-Clean-01.91.svg -o geo

### geo2raw

This is a thin python wrapper for ogr2ogr.

Make sure you have a postgis database set up.  Include the postgis
extensions.  Make sure your versions are fairly recent.  Replace
`dbname`, `user`, `password` with whatever makes sense for you.

This script includes a (fake) closed coastline and benefits coast and
river calculations.

    python geo2raw -i geo -o raw

### raw2ext

This script extracts as much information as possible from the raw
tables and writes them back.  In other words, the raw table contains
the extracted data.  (Maybe this will change in the future.)  The
extraction sequence (`-e`) is always the same, but individual parts
can be left out at your own peril.

Each extraction is committed separately, so you do not have to restart
the whole procedure, but you just need to remove the already committed
part from the extraction list.  On the other hand, if the fault's root
actually lies in a previous step, you probably have to rerun
everything.

Extraction list entries are lower case and white space is not allowed
between entries.

    python raw2ext -i raw -e contours,names,coast,lakes,roads,flora,rivers

#### Contours

This step extracts the contour lines and assigns height labels to the
based on the following heuristics:

* Any label satisfying the regex `\[\^1-9\]\(\[1-9\]\[05\]\|5\)00` is
  a height label.

* the largest number of close by labels wins.

* connect all endpoints of potential contour lines within *EPSL*.

* All unlabeled rings around peaks go in 500ft steps to the outermost
  labeled ring.

* Closed contour lines will be turned into polygons.

The type field in the table contains the elevation.  This heuristic
improves with the number of closed elevation lines.  With Harn being
an island this will eventually decrease when all lines will be closed.

#### Names

Names placed on the map as long as they can automatically be
associated and obtained reasonably well.

#### Coast

Effectively, this is the 0 contour line and this is how it will be
treated in later steps.

This extraction will detect all closed coastlines (including inland
islands) and remove rivers by a simple heuristic.  It will also find
the big lakes that are connected to the coastline; Arain & Tontury
currently.

#### Lakes

This determines all lakes by looking at the fill color.  Elevation of
lakes is not created, calculations are too complex at this point.  In
particular, some have elevation in (currently not recovered) text.

#### Roads

Extract roads as they were intended from the SVG.  It will connect
towns (and such) and roads by modifying lines.  The final roads appear
as *type = 'Trail|Unpaved|Paved'*, but some originals are also modified.

* connect all road end-points to the road network and all road points
  to locations within some distance.

* Remove short end artifacts from the road network.

#### Flora

Turns the WOODLAND, CROPLAND, HEATH, FOREST, NEEDLELEAF, ALPINE,
SNOW_x2F_ICE into multipolygons (in the postgis sense).

Any set at position *n* in this list is reduced by every multipolygon
at later positions.  I.e. the multipolygons are disjoint.  position
*0* is going to be the default, filling all land area not filled
otherwise.

* The above is called "reduce & normalize" in the script.

* Grow patches a bit to cover draw glitches.

* Vegetation is restricted to land.

* Shoal/Reef is restricted to off land.

* The results are in the *polygons* table, type prefixed with `VEG/`.

#### Rivers

Determines rivers from shores to springs in iterations.  Rivers are
created with type *River/n/Mouth:vertex*, where *n* is the level (from
0 at the coast) of detection and *vertex* is *start* or *end*,
depending on the orientation of the linestring.  It includes
directionless lake connectivity, but with level.

### ext2pres

These scripts convert the extracted material into presentation db
tables for each layer.  We assume *0* zoom to show one tile for 360
(by 180) degrees.  Every zoom level takes four times as many tiles
than the previous.  E.g. at zoom level 4, each tile spans 22.5 (by
22.5) degrees.  See https://docs.mapbox.com/help/glossary/zoom-level/.

#### Raster

This step is a computationally involved process.  The script creates a
height field as *raster*.  This version dumps everything into the
table *pres_atlas_raster*.  It adds some fractal "realism", which can
be switched off in the script.

The raster is currently only for the isle of Melderyn, since this step
has the longest duration already by far already and only that part of
Harn has only closed contour lines.

Since not all tools behave weel with raster tables, the rasters are
created as *GeoTIFF* and db table data.

## Presentation

The provided atlas map is best at zooms (256px) tile size of 10km,
which means it can be used for zooms 11-12.  To balance under- and
overscaling artefacts against database size, we create table for the
following zoom ranges. A best guess for Harn nomenclature is attached.
(The *Domain* size is missing in Harn maps and is made up for the
purpose of this project.)

| Zoom  | Label    | High Scale |
| :---: | :------: | :--------: |
|  0-3  | World    | 1 :  70M   | 
|  5-7  | Regional | 1 :   4M   |
|  8-11 | Atlas    | 1 : 250k   |
| 12-15 | Domain   | 1 :  15k   |
| 16-19 | Local    | 1 :   1k   |
| 20-23 | Interior | 1 :  64    |

Raster ids are built as concatenation of as *text* type, separated by
'.'.  Square indexes are always counted in the positive direction.
Rasters start at the Atlas level only.  Longitude is positive only,
because passing a leading '-' as argument is hard.

* 0-360 (longitude) by -90-90 (latitude) - *atlas*
* 0-15 (16x16 lon by lat of 1x1 degree square) - *domain*
* 0-15 (16x16 lon by lat of previous square) - *local*

A typical area specification: `-16.40.0.0,-16.40.15.15`

The whole procedure takes a while, and since Melderyn is the only
completed relevant isle, these are some reasonable calls:

    python ext2pres -i raw -o pres -A 343.40,344.41
    python ext2pres -i raw -o pres -A 344.40.2.6.9.5,344.40.2.6.11.7
