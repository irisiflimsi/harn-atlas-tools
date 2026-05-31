import re
import cv2
import numpy as np
from xml.etree import ElementTree
import fiona
# pylint: disable=no-name-in-module
from fiona.crs import CRS
from shapely.geometry import LineString, mapping, Point, Polygon

SCHEMA_POLYGONS = {'geometry': 'Polygon', 'properties':
                   {'id': 'int', 'type': 'str', 'name': 'str', 'svgid': 'str'}}
SCHEMA_LINES = {'geometry': 'LineString', 'properties':
                {'id': 'int', 'type': 'str', 'name': 'str', 'svgid': 'str'}}
NUM1 = r' ?,?(-?(?:[0-9]*\.?[0-9]+)|(?:[0-9]+))'
NUM2 = NUM1 + NUM1

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

def transform(x, y, width, height):
    """This is where the projection is 'hidden'."""
    pts = (x / width * 0.004 - 15.833738, 40.400725 - y / height * 0.004)
    return pts

def parse(root, poly_file, line_file, width, height):
    """Parse and write everything to the files."""
    for elem in list(root):
        if elem.tag.endswith('path'):
            path = elem.attrib['d']
            typ = elem.attrib['class']
            while len(path) > 0:
                path = path.strip(' ')
                if path.startswith('M'):
                    line = []
                    match = re.match(rf"M{NUM2}", path)
                    x = float(match.group(1))
                    y = float(match.group(2))
                    line.append(transform(x, y, width, height))
                    path = re.sub(rf"M{NUM2}", '', path, 1)
                elif path.startswith('L'):
                    match = re.match(rf"L{NUM2}", path)
                    x = float(match.group(1))
                    y = float(match.group(2))
                    line.append(transform(x, y, width, height))
                    path = re.sub(rf"L{NUM2}", '', path, 1)
                elif path.startswith('h'):
                    path = path[1:]
                    while match := re.match(rf"{NUM1}", path):
                        x += float(match.group(1))
                        line.append(transform(x, y, width, height))
                        path = re.sub(rf"{NUM1}", '', path, 1)
                elif path.startswith('l'):
                    path = path[1:]
                    while match := re.match(rf"{NUM2}", path):
                        x += float(match.group(1))
                        y += float(match.group(2))
                        line.append(transform(x, y, width, height))
                        path = re.sub(rf"{NUM2} ?,?", '', path, 1)
                elif path.startswith('V'):
                    match = re.match(rf"V{NUM1}", path)
                    y = float(match.group(1))
                    line.append(transform(x, y, width, height))
                    path = re.sub(rf"V{NUM1}", '', path, 1)
                elif path.startswith('v'):
                    path = path[1:]
                    while match := re.match(rf"{NUM1}", path):
                        y += float(match.group(1))
                        line.append(transform(x, y, width, height))
                        path = re.sub(rf"{NUM1}", '', path, 1)
                else:
                    print(f"broken path:{path}:")
                    path = ""
        SID.inc_sid()
        if typ == "house":
            line_string = Polygon(line)
            poly_file.write({'geometry': mapping(line_string),
                             'properties': {'id': SID.get_sid(), 'type': typ,
                                            'name': 'local', 'svgid': elem.attrib.get('id', '-')}})
        else: # typ == "road"
            line_string = LineString(line)
            line_file.write({'geometry': mapping(line_string),
                             'properties': {'id': SID.get_sid(), 'type': typ,
                                            'name': 'local', 'svgid': elem.attrib.get('id', '-')}})


in_img = cv2.imread("/storage/rpg/harn/geoserver-originals/local/cherafir.png")
gray_img = cv2.cvtColor(in_img, cv2.COLOR_BGR2GRAY)

_, bin_img = cv2.threshold(gray_img, 150, 255, cv2.THRESH_BINARY)

# Find all the contours
contours, hierarchy = cv2.findContours(bin_img, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)

height, width, channels = in_img.shape

svg = '<?xml version="1.0" encoding="UTF-8"?>'
svg += f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 {width} {height}">'
# Loop through individual contours
for contour in contours:
    # Approximate contour to a polygon
    perimeter = cv2.arcLength(contour, True)
    poly = cv2.approxPolyDP(contour, 0.02 * perimeter, True)

    poly_area = cv2.contourArea(poly)
    poly_len = cv2.arcLength(poly, True)
    # Only choose reasonable sized and "compactness"
    if poly_area > 100 and poly_len * poly_len / poly_area < 30:
        svg += f'  <path class="house" d="M{poly[0][0][0]} {poly[0][0][1]}'
        for pt in poly[1:]:
            svg += f"L{pt[0][0]} {pt[0][1]}"
        svg += '"/>\n'
svg += '  <path class="road" d="M618,1027L594,960L595,888L503,746L348,748L281,706L246,659L281,583L289,238L255,191L243,148L185,97L99,69L63,50L-16,-24L-130,-98"/>\n'
svg += '  <path class="road" d="M618,1027l10,-86l41,-71V728L679,633L681,518L636,451L650,365L629,300L627,273L782,253L793,193l3,-20l23,-73l-2,-110l-2,-93l36,-51l29,-41l16,-47l25,-51l-31,-60l-27,-54v-28l67,-40l51,-27l85,-21l27,-37v-62l60,-83l77,-51l88,-63l94,-89"/>\n'
svg += '  <path class="coast" d="M-124,323h59L-7,339L49,373L65,409L112,488l42,58l50,57l-2,55l26,44l-24,45l-30,111l-40,88l-22,74l7,51l19,49l1,57l-9,36l11,47L198,1314l-12,43l22,55L219,1530v31l-14,125l-23,7l-11,2l2,7l55,1v-16l-2,-2l-7,-66l65,5l45,-34l28,7l-1,21l82,1v63h24v-61l82,-47l17,38l13,-1l-28,-28l72,-53l47,1l1,-21h3l5,88h15l1,-86l42,3L746,1459h5l9,-66l45,-19l19,-47l1,-15l34,-24L847,1236L896,1204L934,1188L959,1124L936,1101L977,1062L961,1020L980,972V932L974,902L954,832l24,-30l-23,-18l37,-44l53,40l-51,68l53,38l11,-13l-42,-32l7,-17l40,30l7,-3l-40,-30l15,-15l34,26l7,-9l-32,-26l15,-22l30,17l5,-15l-42,-26l-13,13L1001,721l27,-37l44,26L1079,682l-48,-28l101,-122l70,47l15,-11L1173,534l56,-37l-67,-47l26,-26l1,-74l28,-68l49,-36l15,-34l61,-72l44,-22l13,-15l49,-11l38,-15l3,-32L1476,3V-19V-42h24l26,34l42,11l30,-11l47,11l45,19l63,30"/>\n'
svg += '</svg>'
print(svg)
root = ElementTree.fromstring(svg)
with fiona.open(f"local_polys.json", 'w', 'GeoJSON', schema=SCHEMA_POLYGONS, crs=CRS.from_epsg(4326)) as poly_file:
    with fiona.open(f"local_lines.json", 'w', 'GeoJSON', schema=SCHEMA_LINES, crs=CRS.from_epsg(4326)) as line_file:
        parse(root, poly_file, line_file, width, height)
