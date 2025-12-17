#!/usr/bin/python
"""
Convert a 'Harn ATlas Map' SVG into GIS format.  Some details are
caused by specific idiosyncracies of such maps.  Also read the help.
"""
import re
import math
import sys
import argparse
from xml.etree import ElementTree
import fiona
from fiona.crs import CRS
import numpy
from shapely.geometry import LineString, mapping, Point, Polygon
from scipy.spatial import distance

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

STYLES = {'-': '-'}
SYMBOLS = {}
SCHEMA_LINES = {'geometry': 'LineString', 'properties':
                {'id': 'int', 'type': 'str', 'len': 'int', 'name': 'str', 'svgid': 'str',
                 'style': 'str'}}
SCHEMA_POINTS = {'geometry': 'Point', 'properties':
                 {'id': 'int', 'type': 'str', 'name': 'str', 'svgid': 'str', 'style': 'str'}}
SCHEMA_POLYGONS = {'geometry': 'Polygon', 'properties':
                   {'id': 'int', 'type': 'str', 'name': 'str', 'svgid': 'str'}}
NUM1 = r' ?,?(-?(?:[0-9]*\.?[0-9]+)|(?:[0-9]+))'
NUM2 = NUM1 + NUM1
NUM4 = NUM2 + NUM2
NUM6 = NUM4 + NUM2

def transform(mat, x_c, y_c):
    """This is where the projection is 'hidden'."""
    pts = ((mat[0]*x_c + mat[2]*y_c + mat[4] - SIZEMINX) / (SIZEMAXX - SIZEMINX) * 14 - 29,
           50 - (mat[1]*x_c + mat[3]*y_c + mat[5] - SIZEMINY) / (SIZEMAXY - SIZEMINY) * 10)
    return pts

def attr2transform(attr):
    """Handle transform attribute."""
    mat = [1, 0, 0, 1, 0, 0]
    mat1 = [1, 0, 0, 1, 0, 0]
    if attr.startswith('matrix'):
        match = re.match(rf"matrix\({NUM6}\)", attr)
        mat = [float(match.group(1)), float(match.group(2)),
               float(match.group(3)), float(match.group(4)),
               float(match.group(5)), float(match.group(6))]
        attr = re.sub(rf"matrix\({NUM6}\)\s*", '', attr, 1)
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

def get_href(typ, elem):
    """Get href and replace with used symbol."""
    if elem.attrib.get('{http://www.w3.org/1999/xlink}href', '-')[1:] in SYMBOLS:
        return SYMBOLS[elem.attrib.get('{http://www.w3.org/1999/xlink}href', '')[1:]]
    return typ

def get_data_name(elem):
    """Get the data-name attribute or the id, if data-name doesn't exist."""
    return elem.attrib.get('data-name', elem.attrib.get('id', '-'))

def parse_point(typ, elem, out_point_file):
    """Parse point and write to file."""
    x_c = y_c = w_c = h_c = 0
    mat = attr2transform(elem.attrib.get('transform', '-'))
    name = get_data_name(elem)
    style = '-'
    if elem.tag.endswith('circle'):
        x_c = float(elem.attrib['cx'])
        y_c = float(elem.attrib['cy'])
        typ += '/' + name
    elif elem.tag.endswith('rect'):
        x_c = float(elem.attrib['x'])
        y_c = float(elem.attrib['y'])
        w_c = float(elem.attrib.get('width', '0'))
        h_c = float(elem.attrib.get('height', '0'))
        typ += '/' + name
    elif elem.tag.endswith('use'):
        x_c = float(elem.attrib.get('x', 0))
        y_c = float(elem.attrib.get('y', 0))
        w_c = float(elem.attrib.get('width', 0))
        h_c = float(elem.attrib.get('height', 0))
        typ += '/' + elem.attrib.get('xlink:href', '-')
        style = re.sub(r".* ","",elem.attrib.get('transform', '-'))
    else:
        print(f"{elem.tag} shouldn't be here")
        return
    typ = get_href(typ, elem)
    pointstring = Point(transform(mat, x_c + w_c/2., y_c + h_c/2.))
    SID.inc_sid()
    out_point_file.write({'geometry': mapping(pointstring),
                          'properties': {'id': SID.get_sid(), 'type': typ,
                                         'name': name, 'svgid': elem.attrib.get('id', '-'),
                                         'style': style}})

def parse_path(typ, elem, out_lines_file, out_point_file):
    """Parse path and write to file as line."""
    x_c = y_c = x0_c = y0_c = xb_c = yb_c = 0
    mat = [1, 0, 0, 1, 0, 0]
    line = []
    mat = attr2transform(elem.attrib.get('transform', '-'))
    name = get_data_name(elem)
    typ += '/' + name
    typ = get_href(typ, elem)
    path = elem.attrib['d']
    while len(path) > 0:
        path = path.strip(' ')
        if path.startswith('M'):
            if len(line) > 0:
                out_line(line, typ, name, out_lines_file, elem)
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
            out_line(line, typ, name, out_lines_file, elem)
            line = []
        elif path.startswith('c'):
            # Specific copy symbol
            if "c0,1.24-1.01,2.25-2.25,2.25s-2.25-1.01-2.25-2.25,1.01-2.25,2.25-2.25,2.25,1.01,2.25,2.25Z" in path:
                print(f"special path for: {name}")
                x_c += 5.5
                y_c += 7.5
                w_c = h_c = 4.5
                pointstring = Point(transform(mat, x_c + w_c/2., y_c + h_c/2.))
                SID.inc_sid()
                out_point_file.write({'geometry': mapping(pointstring), 'properties':
                                      {'id': SID.get_sid(), 'type': 'special copy',
                                       'name': name, 'svgid': elem.attrib.get('id', '-'),
                                       'style': '-'}})
                return
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
            print(f"broken path:{path}:")
            path = ""
    out_line(line, typ, name, out_lines_file, elem)

def out_line(line, typ, name, out_lines_file, elem):
    """Terminate a line in path."""
    if len(line) > 1:
        line_string = LineString(line)
        SID.inc_sid()
        out_lines_file.write(
            {'geometry': mapping(line_string),
             'properties': {'id': SID.get_sid(), 'type': typ, 'len': len(line),
                            'name': name, 'svgid': elem.attrib.get('id', '-'),
                            'style': STYLES[elem.attrib.get('class', '-')]}})

def parse_polygon(typ, elem, out_polygon_file):
    """Parse polygon and write to file."""
    x_c = y_c = 0
    mat = [1, 0, 0, 1, 0, 0]
    line = []
    name = get_data_name(elem)
    typ += '/' + name
    mat = attr2transform(elem.attrib.get('transform', '-'))
    points = elem.attrib['points'].strip(' ').replace(',', ' ').split(' ')
    while len(points) > 1:
        x_c = float(points[0])
        y_c = float(points[1])
        points = points[2:]
        line.append(transform(mat, x_c, y_c))
    typ = get_href(typ, elem)
    if len(line) > 1:
        polygon = Polygon(line)
        SID.inc_sid()
        out_polygon_file.write({'geometry': mapping(polygon),
                                'properties': {'id': SID.get_sid(), 'type': typ,
                                               'name': name, 'svgid': elem.attrib.get('id', '-')}})
    else:
        print(f"pathological:{SID.get_sid()}")

def parse_line(typ, elem, out_lines_file):
    """Parse line and write to file."""
    x_c = y_c = 0
    mat = [1, 0, 0, 1, 0, 0]
    line = []
    name = get_data_name(elem)
    typ += '/' + name
    mat = attr2transform(elem.attrib.get('transform', '-'))
    if elem.tag.endswith('polyline'):
        points = elem.attrib['points'].strip(' ').replace(',', ' ').split(' ')
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
        typ += '/' + name
        line = [transform(mat, x1_c, y1_c), transform(mat, x2_c, y2_c)]
    else:
        print(f"{elem.tag} shouldn't be here")
        return
    typ = get_href(typ, elem)
    if len(line) > 1:
        line_string = LineString(line)
        SID.inc_sid()
        out_lines_file.write(
            {'geometry': mapping(line_string),
             'properties': {'id': SID.get_sid(), 'type': typ,
                            'len': len(line), 'name': name, 'svgid': name,
                            'style': STYLES[elem.attrib.get('class', '-')]}})
    else:
        print(f"pathological:{SID.get_sid()}")

def parse_symbol(args, elem):
    """Parse symbols."""
    if args.verbose:
        print(f"parsing {elem.tag} with id={elem.attrib.get('id', '')} " + \
              f"and data-name={elem.attrib.get('data-name', '')}")
    if elem.attrib.get('id', '-') != '-':
        SYMBOLS[elem.attrib.get('id', '')] = get_data_name(elem)

def parse_style(args, text):
    """Parse all styles. Poor man's parsing."""
    keys = []
    for line in text.splitlines():
        line = line.strip(' ')
        if line == "":
            pass
        elif line.startswith("."):
            keys = [key[:-1] if key[-1] == ',' else key for key in line.split(' ')[:-1]]
            if args.verbose:
                print(f"parsing current style keys {keys}")
        elif line.startswith("}"):
            pass
        else:
            if args.verbose:
                print(f"parsing current style value {line}")
            for key in keys:
                if key[1:] in STYLES:
                    STYLES[key[1:]] += line
                else:
                    STYLES[key[1:]] = line

def parse(args, name, root, out_polygon_file, out_point_file, out_lines_file):
    """Parse and write everything to the files."""
    for elem in list(root):
        if elem.tag.endswith('polygon'):
            parse_polygon(name, elem, out_polygon_file)
        elif elem.tag.endswith('path'):
            parse_path(name, elem, out_lines_file, out_point_file)
        elif elem.tag.endswith('polyline'):
            parse_line(name, elem, out_lines_file)
        elif elem.tag.endswith('line'):
            parse_line(name, elem, out_lines_file)
        elif elem.tag.endswith('use'):
            parse_point(name, elem, out_point_file)
        elif elem.tag.endswith('rect'):
            parse_point(name, elem, out_point_file)
        elif elem.tag.endswith('circle'):
            parse_point(name, elem, out_point_file)
        elif elem.tag.endswith('defs'):
            parse(args, name, elem, out_polygon_file, out_point_file, out_lines_file)
        elif elem.tag.endswith('symbol'):
            parse_symbol(args, elem)
        elif elem.tag.endswith('g'):
            # Some of this stuff isn't really necessary
            if get_data_name(elem) not in ['GRID_NUMBERS', 'KINGDOM_MAPS',
                                           'ATLAS_MAPS', 'MAP_GRIDS', 'HEXES']:
                parse(args, f"{name}/{get_data_name(elem)}", elem,
                      out_polygon_file, out_point_file, out_lines_file)
        elif elem.tag.endswith('MetaInfo'):
            pass
        elif elem.tag.endswith('text'):
            pass
        elif elem.tag.endswith('mask'):
            pass
        elif elem.tag.endswith('clipPath'):
            pass
        elif elem.tag.endswith('pattern'):
            pass
        elif elem.tag.endswith('linearGradient'):
            pass
        elif elem.tag.endswith('style'):
            parse_style(args, elem.text)
        elif elem.tag.endswith('image'):
            pass
        else:
            print(f"{elem.tag} not expected")

def main():
    """Main method."""
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Convert Harn SVG to a few GIS formats.  ' +
        'Use ogr2ogr to convert to other formats not compiled into fiona.')
    parser.add_argument('-i', '--input', dest='infile', help='input file name',
                        required=True)
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose',
                        required=False)
    parser.add_argument('-o', '--output', dest='outfile', help='output file name',
                        required=True)
    parser.add_argument('-t', '--test', action='store_true', help='run tests instead',
                        required=False)
    args = parser.parse_args()

    if args.test:
        if args.verbose:
            print("test simple scale 1")
        assert attr2transform('scale(2,3)') == [2, 0, 0, 3, 0, 0], "simple scale 1"
        if args.verbose:
            print("test simple scale 2")
        assert attr2transform('scale(2)') == [2, 0, 0, 1, 0, 0], "simple scale 2"
        if args.verbose:
            print("test simple matrix")
        assert attr2transform('matrix(2 3 4 5 6 7)') == [2, 3, 4, 5, 6, 7], "simple matrix"
        if args.verbose:
            print("test simple translate 1")
        assert attr2transform('translate(2 3)') == [1, 0, 0, 1, 2, 3], "simple translate 1"
        if args.verbose:
            print("test simple translate 2")
        assert attr2transform('translate(2)') == [1, 0, 0, 1, 2, 0], "simple translate 2"
        if args.verbose:
            print("test rotate")
        assert numpy.allclose(attr2transform('rotate(30)'),
                              [.866, .5, -.5, .866, 0, 0],
                              atol=1e-3), f"simple matrix={attr2transform('rotate(30)')}"
        if args.verbose:
            print("test rotate translate")
        assert numpy.allclose(attr2transform('rotate(90) translate(1)'),
                              [0, 1, -1, 0, 0, 1], atol=1e-3), f"order rotate translate"
        if args.verbose:
            print("test translate rotate")
        assert numpy.allclose(attr2transform('translate(1) rotate(90)'),
                              [0, 1, -1, 0, 1, 0], atol=1e-3), "order translate rotate"
        # Test special curve variants. Eyeball output.
        if args.verbose:
            print("test curves in svg paths")
        path = "M10,10C20,20 30,20 40,20c10,-10 20,-10 30,0" + \
            "s10,10 10,0q10,-10 10,0t10,0Z"
        svg = f'<svg><path d="{path}" stroke="black" stroke-width=".01" ' + \
            'fill="transparent"/></svg>'
        with open("unittest.svg", 'w') as svg_test_out_file:
            print(svg, file=svg_test_out_file)
        with fiona.open("unittest.json", 'w', 'GeoJSON', schema=SCHEMA_LINES,
                        crs=CRS.from_epsg(4326)) as json_test_out_file:
            elem = ElementTree.fromstring(svg)[0]
            parse_path("type", elem, json_test_out_file, None)

    else:
        root = ElementTree.parse(args.infile).getroot()
        el_a1 = root.find(".//*[@id='A1']")
        if el_a1 is None:
            el_a1 = root.find(".//*[@data-name='A1']")
        global SIZEMINX
        print(el_a1)
        SIZEMINX = float(el_a1.attrib.get('x', 0))
        global SIZEMINY
        SIZEMINY = float(el_a1.attrib.get('y', 0))
        global SIZEMAXX
        SIZEMAXX = float(el_a1.attrib.get('x', 0)) + 14 * float(el_a1.attrib.get('width', 0))
        global SIZEMAXY
        SIZEMAXY = float(el_a1.attrib.get('y', 0)) + 10 * float(el_a1.attrib.get('height', 0))
        if args.outfile.endswith('.shp'):
            if args.verbose:
                print("output ESRI shapefile")
            prefix = args.outfile[:-4]
            ext = 'shp'
            outformat = 'ESRI Shapefile'
        elif args.outfile.endswith('.json'):
            if args.verbose:
                print("output GeoJSON")
            prefix = args.outfile[:-5]
            ext = 'json'
            outformat = 'GeoJSON'
        else:
            print("Unkown extension, only .json (GeoJSON) or .shp (ESRI Shapefile) allowed. " +
                  "Use ogr2ogr for other formats.")
            sys.exit(-1)

        with fiona.open(f"{prefix}_polys.{ext}", 'w', outformat,
                        schema=SCHEMA_POLYGONS, crs=CRS.from_epsg(4326)) as out_polygon_file:
            with fiona.open(f"{prefix}_pts.{ext}", 'w', outformat,
                            schema=SCHEMA_POINTS, crs=CRS.from_epsg(4326)) as out_point_file:
                with fiona.open(f"{prefix}_lines.{ext}", 'w', outformat,
                                schema=SCHEMA_LINES, crs=CRS.from_epsg(4326)) as out_lines_file:
                    parse(args, '', root, out_polygon_file, out_point_file, out_lines_file)

if __name__ == '__main__':
    main()
