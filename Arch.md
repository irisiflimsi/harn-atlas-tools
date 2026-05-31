```mermaid
flowchart TD
    A(Original) -- export -->  B[SVG]
    B           -- svg2geo --> C[GeoJSON]
    C           -- ogr2ogr --> D[SQL Tables 1]
    D           -- geo_* -->   E[SQL Tables 2]
    
    E1(SQL Tables 3) -- static partition<br><i>Martin CLI</i> --> F1[Tiles]
    F1               -- serve                                 --> G1[Browser]
    G1               -- <i>MapLibre</i>                       --> H(Maps!)

    E1 -- dynamic partition<br><i>TileServer</I> --> F2[Tiles]
    F2 -- serve                                  --> G1

    E1 -- dynamic convert<br><i>GeoServer</i>   --> F3[WMS]
    F3 -- dynamic partition<br><i>GeoServer</i> --> F2
    G2 -- <i>OpenLayers</i>                     --> H

    F3 -- serve --> G2[Browser]
```

The left image is straightforward, there are little choices, apart from the schemas
involved.  They can quickly be revisited and easily altered.

The right, depicting the way the raw data reaches the end user, shows choices that
need to be made that will have wasted a lot of effort, if changed at a later time.
The simplest but most rigid solution is on the left, the most complex providing
many flexible extensions on the right.

A SW package is named only as example, not as the only SW option. (Finding SW
options and how to configure such a SW is a main piece of the "wasted effort if
changed".)

## SQL Tables 2 -> 3

Note that the structure of the DB data also depends on the way it is being served.
I therefore added a DB restructuring step not shown in the diagram.
