#!/usr/bin/python
"""
Convert a 'Harn Local Map' SVG into GeoJSON format.  Currently
only avaibale for Cherafir.  Also read the help.
"""
import re
import sys
import argparse
import math
import logging
import xml
from dataclasses import dataclass
import fiona
# pylint: disable=no-name-in-module
from fiona.crs import CRS
from shapely.geometry import LineString, mapping, Point, Polygon
from scipy.spatial import distance

@dataclass
class Outfiles:
    """Encapsulate all out files."""
    polygons = None
    lines = None
    points = None

class SID:
    """Encapsulate non-final global variable."""
    sid = 0
    @classmethod
    def inc_sid(cls):
        """Increment sid."""
        cls.sid += 1
    @classmethod
    def get_sid(cls):
        """Get sid."""
        return cls.sid

# Schemas
LINES = {'geometry': 'LineString', 'properties': {'id': 'int', 'type': 'str'}}
POINTS = {'geometry': 'Point', 'properties': {'id': 'int', 'type': 'str'}}
POLYGONS = {'geometry': 'Polygon', 'properties': {'id': 'int', 'type': 'str'}}

# Regex
NUM1 = r' ?,?(-?(?:[0-9]*\.?[0-9]+)|(?:[0-9]+))'
NUM1M = r' ?,?([^ ,]+)'
NUM2 = NUM1 + NUM1
NUM4 = NUM2 + NUM2
NUM6 = NUM4 + NUM2
NUM6M = NUM1M + NUM1M + NUM1M + NUM1M + NUM1M + NUM1M

LOGGER = logging.getLogger(__name__)

DEFS = {}
PX_100FT = 1
FT_DEG = 3*100*1100

CENTERS = {}
CENTERS["Cherafir"] = (-15.833, 40.400)

def transform(mat, x_c, y_c):
    """This is where the projection is 'hidden'."""
    pts = (
        CENTERS["Cherafir"][0] + (mat[0]*x_c + mat[2]*y_c + mat[4]) / PX_100FT * 100 / FT_DEG,
        CENTERS["Cherafir"][1] - (mat[1]*x_c + mat[3]*y_c + mat[5]) / PX_100FT * 100 / FT_DEG
    )
    return pts

def transform_r(mat, r):
    """This is where the projection is 'hidden'."""
    # We do not treat ellipses!
    return mat[0]*r / PX_100FT * 100 / FT_DEG

def attr2transform(attr):
    """Handle transform attribute."""
    mat = [1, 0, 0, 1, 0, 0]
    mat1 = [1, 0, 0, 1, 0, 0]
    if attr.startswith('matrix'):
        match = re.match(rf"matrix\({NUM6M}\)", attr)
        mat = [float(match.group(1)), float(match.group(2)),
               float(match.group(3)), float(match.group(4)),
               float(match.group(5)), float(match.group(6))]
        attr = re.sub(rf"matrix\({NUM6M}\)\s*", '', attr, 1)
        mat1 = attr2transform(attr)
    elif attr.startswith('translate'):
        match = re.match(rf"translate\({NUM2}\)", attr)
        if match is None:
            match = re.match(rf"translate\({NUM1}\)", attr)
            mat = [1, 0, 0, 1, float(match.group(1)), 0]
            attr = re.sub(rf"translate\({NUM1}\)\s*", '', attr, 1)
        else:
            mat = [1, 0, 0, 1, float(match.group(1)), float(match.group(2))]
            attr = re.sub(rf"translate\({NUM2}\)\s*", '', attr, 1)
        mat1 = attr2transform(attr)
    elif attr.startswith('scale'):
        match = re.match(rf"scale\({NUM2}\)", attr)
        if match is None:
            match = re.match(rf"scale\({NUM1}\)", attr)
            mat = [float(match.group(1)), 0, 0, 1, 0, 0]
            attr = re.sub(rf"scale\({NUM1}\)\s*", '', attr, 1)
        else:
            mat = [float(match.group(1)), 0, 0, float(match.group(2)), 0, 0]
            attr = re.sub(rf"scale\({NUM2}\)\s*", '', attr, 1)
        mat1 = attr2transform(attr)
    elif attr.startswith('rotate'):
        match = re.match(rf"rotate\({NUM1}\)", attr)
        cos = math.cos(math.pi * float(match.group(1)) / 180)
        sin = math.sin(math.pi * float(match.group(1)) / 180)
        mat = [cos, sin, -sin, cos, 0, 0]
        attr = re.sub(rf"rotate\({NUM1}\)\s*", '', attr, 1)
        mat1 = attr2transform(attr)
    else:
        return mat
    mat = [mat[0]*mat1[0] + mat[2]*mat1[1],
           mat[1]*mat1[0] + mat[3]*mat1[1],
           mat[0]*mat1[2] + mat[2]*mat1[3],
           mat[1]*mat1[2] + mat[3]*mat1[3],
           mat[0]*mat1[4] + mat[2]*mat1[5] + mat[4],
           mat[1]*mat1[4] + mat[3]*mat1[5] + mat[5]]
    return mat

def get_href(elem):
    """Get href and replace with used def."""
    return DEFS[elem.attrib.get('{http://www.w3.org/1999/xlink}href', '-')[1:]]

def parse_path(elem, outfiles, defs):
    """Parse path and write to file as line."""
    x_c = y_c = x0_c = y0_c = xb_c = yb_c = 0
    mat = [1, 0, 0, 1, 0, 0]
    line = []
    mat = attr2transform(elem.attrib.get('transform', '-'))
    path = elem.attrib['d']
    while len(path) > 0:
        path = path.strip(' ')
        if path.startswith('M'):
            if len(line) > 0:
                out_line(line, outfiles, elem, defs)
            line = []
            match = re.match(rf"M{NUM2}", path)
            x_c = float(match.group(1))
            y_c = float(match.group(2))
            xb_c = x0_c = x_c
            yb_c = y0_c = y_c
            line.append(transform(mat, x_c, y_c))
            path = re.sub(rf"M{NUM2}", '', path, 1)
        elif path.startswith('L'):
            match = re.match(rf"L{NUM2}", path)
            xb_c = x_c = float(match.group(1))
            yb_c = y_c = float(match.group(2))
            line.append(transform(mat, x_c, y_c))
            path = re.sub(rf"L{NUM2}", '', path, 1)
        elif path.startswith('l'):
            path = path[1:]
            while match := re.match(rf"{NUM2}", path):
                x_c += float(match.group(1))
                y_c += float(match.group(2))
                line.append(transform(mat, x_c, y_c))
                path = re.sub(rf"{NUM2} ?,?", '', path, 1)
            xb_c = x_c
            yb_c = y_c
        elif path.startswith('V'):
            match = re.match(rf"V{NUM1}", path)
            yb_c = y_c = float(match.group(1))
            line.append(transform(mat, x_c, y_c))
            path = re.sub(rf"V{NUM1}", '', path, 1)
        elif path.startswith('v'):
            path = path[1:]
            while match := re.match(rf"{NUM1}", path):
                y_c += float(match.group(1))
                line.append(transform(mat, x_c, y_c))
                path = re.sub(rf"{NUM1}", '', path, 1)
            yb_c = y_c
        elif path.startswith('H'):
            match = re.match(rf"H{NUM1}", path)
            xb_c = x_c = float(match.group(1))
            line.append(transform(mat, x_c, y_c))
            path = re.sub(rf"H{NUM1}", '', path, 1)
        elif path.startswith('h'):
            path = path[1:]
            while match := re.match(rf"{NUM1}", path):
                x_c += float(match.group(1))
                line.append(transform(mat, x_c, y_c))
                path = re.sub(rf"{NUM1}", '', path, 1)
            xb_c = x_c
        elif path.startswith('Z'):
            line.append(transform(mat, x0_c, y0_c))
            path = path[1:]
            out_line(line, outfiles, elem, defs)
            line = []
        elif path.startswith('z'):
            line.append(transform(mat, x0_c, y0_c))
            path = path[1:]
            out_line(line, outfiles, elem, defs)
            line = []
        elif path.startswith('c'):
            path = path[1:]
            while match := re.match(rf"{NUM6}", path):
                x1_c = x_c
                y1_c = y_c
                x2_c = x1_c + float(match.group(1))
                y2_c = y1_c + float(match.group(2))
                xb_c = x1_c + float(match.group(3))
                yb_c = y1_c + float(match.group(4))
                x_c = x1_c + float(match.group(5))
                y_c = y1_c + float(match.group(6))
                dist = distance.euclidean((x_c, y_c), (x1_c, y1_c))
                for t_c in range(1, math.floor(dist)):
                    tdf = float(t_c/dist)
                    xt_c = pow(1 - tdf, 3)*x1_c + 3*pow(1 - tdf, 2)*(tdf)*x2_c + \
                        3*(1 - tdf)*pow(tdf, 2)*xb_c + pow(tdf, 3)*x_c
                    yt_c = pow(1 - tdf, 3)*y1_c + 3*pow(1 - tdf, 2)*(tdf)*y2_c + \
                        3*(1 - tdf)*pow(tdf, 2)*yb_c + pow(tdf, 3)*y_c
                    line.append(transform(mat, xt_c, yt_c))
                line.append(transform(mat, x_c, y_c))
                path = re.sub(rf"{NUM6} ?,?", '', path, 1)
        elif path.startswith('s'):
            path = path[1:]
            while match := re.match(rf"{NUM4}", path):
                x1_c = x_c
                y1_c = y_c
                x2_c = x_c + (x_c - xb_c)
                y2_c = y_c + (y_c - yb_c)
                xb_c = x1_c + float(match.group(1))
                yb_c = y1_c + float(match.group(2))
                x_c = x1_c + float(match.group(3))
                y_c = y1_c + float(match.group(4))
                dist = distance.euclidean((x_c, y_c), (x1_c, y1_c))
                for t_c in range(1, math.floor(dist)):
                    tdf = float(t_c/dist)
                    xt_c = pow(1 - tdf, 3)*x1_c + 3*pow(1 - tdf, 2)*(tdf)*x2_c + \
                        3*(1 - tdf)*pow(tdf, 2)*xb_c + pow(tdf, 3)*x_c
                    yt_c = pow(1 - tdf, 3)*y1_c + 3*pow(1 - tdf, 2)*(tdf)*y2_c + \
                        3*(1 - tdf)*pow(tdf, 2)*yb_c + pow(tdf, 3)*y_c
                    line.append(transform(mat, xt_c, yt_c))
                line.append(transform(mat, x_c, y_c))
                path = re.sub(rf"{NUM4} ?,?", '', path, 1)
        elif path.startswith('C'):
            path = path[1:]
            while match := re.match(rf"{NUM6}", path):
                x1_c = x_c
                y1_c = y_c
                x2_c = float(match.group(1))
                y2_c = float(match.group(2))
                xb_c = float(match.group(3))
                yb_c = float(match.group(4))
                x_c = float(match.group(5))
                y_c = float(match.group(6))
                dist = distance.euclidean((x_c, y_c), (x1_c, y1_c))
                for t_c in range(1, math.floor(dist)):
                    tdf = float(t_c/dist)
                    xt_c = pow(1 - tdf, 3)*x1_c + 3*pow(1 - tdf, 2)*(tdf)*x2_c + \
                        3*(1 - tdf)*pow(tdf, 2)*xb_c + pow(tdf, 3)*x_c
                    yt_c = pow(1 - tdf, 3)*y1_c + 3*pow(1 - tdf, 2)*(tdf)*y2_c + \
                        3*(1 - tdf)*pow(tdf, 2)*yb_c + pow(tdf, 3)*y_c
                    line.append(transform(mat, xt_c, yt_c))
                line.append(transform(mat, x_c, y_c))
                path = re.sub(rf"{NUM6} ?,?", '', path, 1)
        elif path.startswith("q"):
            path = path[1:]
            while match := re.match(rf"{NUM4}", path):
                x1_c = x_c
                y1_c = y_c
                xb_c = x1_c + float(match.group(1))
                yb_c = y1_c + float(match.group(2))
                x_c = x1_c + float(match.group(3))
                y_c = y1_c + float(match.group(4))
                dist = distance.euclidean((x_c, y_c), (x1_c, y1_c))
                for t_c in range(1, math.floor(dist)):
                    tdf = float(t_c/dist)
                    xt_c = pow(1 - tdf, 2)*x1_c + 2*(1 - tdf)*(tdf)*xb_c + pow(tdf, 2)*x_c
                    yt_c = pow(1 - tdf, 2)*y1_c + 2*(1 - tdf)*(tdf)*yb_c + pow(tdf, 2)*y_c
                    line.append(transform(mat, xt_c, yt_c))
                line.append(transform(mat, x_c, y_c))
                path = re.sub(rf"{NUM4} ?,?", '', path, 1)
        elif path.startswith('t'):
            path = path[1:]
            while match := re.match(rf"{NUM2}", path):
                x1_c = x_c
                y1_c = y_c
                xb_c = x_c + (x_c - xb_c)
                yb_c = y_c + (y_c - yb_c)
                x_c = x1_c + float(match.group(1))
                y_c = y1_c + float(match.group(2))
                dist = distance.euclidean((x_c, y_c), (x1_c, y1_c))
                for t_c in range(1, math.floor(dist)):
                    tdf = float(t_c/dist)
                    xt_c = pow(1 - tdf, 2)*x1_c + 2*(1 - tdf)*(tdf)*xb_c + pow(tdf, 2)*x_c
                    yt_c = pow(1 - tdf, 2)*y1_c + 2*(1 - tdf)*(tdf)*yb_c + pow(tdf, 2)*y_c
                    line.append(transform(mat, xt_c, yt_c))
                line.append(transform(mat, x_c, y_c))
                path = re.sub(rf"{NUM2} ?,?", '', path, 1)
        else:
            LOGGER.error("broken path:%s:", path)
            path = ""
    out_line(line, outfiles, elem, defs)

def get_attributes(elem):
    """Get standard attibutes."""
    typ = f"fill:{elem.attrib.get('fill', '-')};"
    typ += f"stroke:{elem.attrib.get('stroke', '-')};"
    typ += f"stroke-width:{elem.attrib.get('stroke-width', '1')};"
    return typ

def out_line(line, outfiles, elem, defs):
    """Terminate a line in path."""
    if len(line) > 1:
        line_string = LineString(line)
        SID.inc_sid()
        typ = get_attributes(elem)
        if defs:
            DEFS[elem.attrib['id']] = line_string
        else:
            outfiles.lines.write(
                {'geometry': mapping(line_string), 'properties': {'id': SID.get_sid(), 'type': typ}}
            )

def parse_polygon(elem, outfiles):
    """Parse polygon and write to file."""
    x_c = y_c = 0
    mat = [1, 0, 0, 1, 0, 0]
    line = []
    mat = attr2transform(elem.attrib.get('transform', '-'))
    points = re.compile(r'\s+').sub(' ', elem.attrib['points']).replace(',', ' ').split(' ')
    if points[-1] == '':
        points = points[:-1]
    while len(points) > 1:
        x_c = float(points[0])
        y_c = float(points[1])
        points = points[2:]
        line.append(transform(mat, x_c, y_c))
    if len(line) > 1:
        polygon = Polygon(line)
        SID.inc_sid()
        typ = get_attributes(elem)
        outfiles.polygons.write(
            {'geometry': mapping(polygon), 'properties': {'id': SID.get_sid(), 'type': typ}}
        )
    else:
        LOGGER.warning("ignoring polygon: %s", points)

def parse_rect(elem, outfiles, defs):
    """Parse polygon and write to file."""
    line = []
    mat = attr2transform(elem.attrib.get('transform', '-'))
    x_1 = float(elem.attrib.get('x', 0))
    y_1 = float(elem.attrib.get('y', 0))
    x_2 = x_1 + float(elem.attrib.get('width', 0))
    y_2 = y_1 + float(elem.attrib.get('height', 0))

    line.append(transform(mat, x_1, y_1))
    line.append(transform(mat, x_1, y_2))
    line.append(transform(mat, x_2, y_2))
    line.append(transform(mat, x_2, y_1))
    polygon = Polygon(line)
    SID.inc_sid()
    typ = get_attributes(elem)
    if defs:
        DEFS[elem.attrib['id']] = polygon
    else:
        outfiles.polygons.write(
            {'geometry': mapping(polygon), 'properties': {'id': SID.get_sid(), 'type': typ}}
        )

def parse_clip(elem, outfiles, root):
    """Parse a clippath."""
    string = get_href(elem.findall('*')[0])
    group = root.findall(f".//*[@clip-path='url(#{elem.attrib['id']})']")
    if group is not None:
        typ = group[0].attrib.get('id', '-')
        SID.inc_sid()
        if isinstance(string, LineString):
            outfiles.lines.write(
                {'geometry': mapping(string), 'properties': {'id': SID.get_sid(), 'type': typ}}
            )
        else:
            outfiles.polygons.write(
                {'geometry': mapping(string), 'properties': {'id': SID.get_sid(), 'type': typ}}
            )

def parse_point(elem, outfiles):
    """Parse point and write to file."""
    x_c = y_c = w_c = h_c = 0
    typ = '-'
    mat = attr2transform(elem.attrib.get('transform', '-'))
    if elem.tag.endswith('circle'):
        x_c = float(elem.attrib['cx'])
        y_c = float(elem.attrib['cy'])
        typ = f"radius:{transform_r(mat, float(elem.attrib['r']))};"
        typ += get_attributes(elem)
    elif elem.tag.endswith('text'):
        x_c = float(elem.attrib.get('x', 0))
        y_c = float(elem.attrib.get('y', 0))
        typ = elem.text if elem.text is not None else ''
        for child in elem.findall('*'): # all tspan
            typ += " " + child.text
    elif elem.tag.endswith('ellipse'):
        x_c = float(elem.attrib['cx'])
        y_c = float(elem.attrib['cy'])
        typ = 'well'
    else:
        LOGGER.warning("ignoring element %s", elem)
    pointstring = Point(transform(mat, x_c + w_c/2., y_c + h_c/2.))
    SID.inc_sid()
    outfiles.points.write(
        {'geometry': mapping(pointstring), 'properties': {'id': SID.get_sid(), 'type': typ}}
    )

def parse_line(elem, outfiles):
    """Parse line and write to file."""
    x_c = y_c = 0
    mat = [1, 0, 0, 1, 0, 0]
    line = []
    mat = attr2transform(elem.attrib.get('transform', '-'))
    if elem.tag.endswith('polyline'):
        points = re.compile(r'\s+').sub(' ', elem.attrib['points']).replace(',', ' ').split(' ')
        if points[-1] == '':
            points = points[:-1]
        while len(points) > 1:
            x_c = float(points[0])
            y_c = float(points[1])
            points = points[2:]
            line.append(transform(mat, x_c, y_c))
    elif elem.tag.endswith('line'):
        x1_c = float(elem.attrib['x1'])
        y1_c = float(elem.attrib['y1'])
        x2_c = float(elem.attrib['x2'])
        y2_c = float(elem.attrib['y2'])
        line = [transform(mat, x1_c, y1_c), transform(mat, x2_c, y2_c)]
    else:
        LOGGER.error("%s shouldn't be here", elem.tag)
        return
    if len(line) > 1:
        line_string = LineString(line)
        SID.inc_sid()
        typ = get_attributes(elem)
        outfiles.lines.write(
            {'geometry': mapping(line_string), 'properties': {'id': SID.get_sid(), 'type': typ}}
        )
    else:
        LOGGER.error("pathological:%s", SID.get_sid())

def parse(args, root, outfiles, real_root, defs=False):
    """Parse and write everything to the files."""
    # Replace
    for elem in list(root):
        if elem.tag.endswith('polygon') and not defs:
            parse_polygon(elem, outfiles)
        elif elem.tag.endswith('line') and not defs:
            parse_line(elem, outfiles)
        elif elem.tag.endswith('circle') and not defs:
            parse_point(elem, outfiles)
        elif elem.tag.endswith('ellipse') and not defs:
            parse_point(elem, outfiles)
        elif elem.tag.endswith('defs') and not defs:
            parse(args, elem, outfiles, real_root, True)
        elif elem.tag.endswith('g') and not defs:
            if elem.attrib.get('id', '') != 'Legend':
                parse(args, elem, outfiles, real_root)
        elif elem.tag.endswith('text') and not defs:
            parse_point(elem, outfiles)
        elif elem.tag.endswith('mask') and not defs:
            pass
        elif elem.tag.endswith('clipPath') and not defs:
            parse_clip(elem, outfiles, real_root)
        elif elem.tag.endswith('use') and not defs:
            pass
        elif elem.tag.endswith('feFlood') and not defs:
            pass
        elif elem.tag.endswith('image') and not defs:
            pass
        elif elem.tag.endswith('pgf') and not defs:
            pass
        elif elem.tag.endswith('feBlend') and not defs:
            pass
        elif elem.tag.endswith('filter', defs) and defs:
            parse(args, elem, outfiles, real_root)
        elif elem.tag.endswith('rect'):
            parse_rect(elem, outfiles, defs)
        elif elem.tag.endswith('path', defs):
            parse_path(elem, outfiles, defs)
        elif elem.tag.endswith('switch', defs):
            parse(args, elem, outfiles, real_root)
        else:
            LOGGER.error("%s not expected", elem.tag)

def main(args):
    """Main method."""
    LOGGER.info("Complete parse...")
    root = xml.etree.ElementTree.parse(args.infile).getroot()
    crs = CRS.from_epsg(4326)
    outfiles = Outfiles()
    fname = f"{args.outpre}_polygons.json"
    with fiona.open(fname, 'w', 'GeoJSON', schema=POLYGONS, crs=crs) as outfiles.polygons:
        fname = f"{args.outpre}_points.json"
        with fiona.open(fname, 'w', 'GeoJSON', schema=POINTS, crs=crs) as outfiles.points:
            fname = f"{args.outpre}_lines.json"
            with fiona.open(fname, 'w', 'GeoJSON', schema=LINES, crs=crs) as outfiles.lines:
                # Cherafir legend
                legend = root.find(".//*[@id='Symbol_5_']/..")
                legend.set('id', 'Legend')
                global PX_100FT
                PX_100FT = (56.897 + 0.25) / 2 # (stroke-width+width)/2
                parse(args, root, outfiles, root)

if __name__ == '__main__':
    main()
