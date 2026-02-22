#!/usr/bin/python
"""
Extract additional information
"""
import argparse
import sys
import re

LETTERS = {
    "c0-.6.0": "0",
    "h-.33": "1",
    "v.42": "2",
    "l.32-.0": "3",
    "v-.86s-": "4",
    "l.34-.0": "5",
    "l-.32.0": "6",
    "v-.42[sh]1.7": "7",
    "c-.13-.0": "8",
    "l.31-.04c": "9",
    "l1.0": "A",
    "v-3.58s1,0,1,0c.2,0,.36.04.48.1": "B",
    "l.35.12": "C",
    "v-3.58s.92,0,.92,0c.2": "D",
    "v-3.58s1.92,0,1.92,0": "E",
    "v-3.58s1.79": "F",
    "v-.42[hs]1.1": "G",
    "v-3.58s.36,0,.36,0v1.47s": "H",
    "v-3.58s.36,0,.36,0v3.58s": "I",
    "v-3.58h.35v3.58h-.35Z": "I",
    "l.31-.06c0": "J",
    "v-3.58s.36,0,.36,0v1.7": "K",
    "v-3.58h.35v1": "K",
    "v-3.58s.36,0,.36,0v3.16s": "L",
    "v-3.58s.54": "M",
    "v-3.58s.37": "N",
    "v-3.58h.36": "N",
    "c0-.61.": "O",
    "v-3.58s1.01": "P",
    "c.16.15": "Q",
    "v-3.58[hs]1.1": "R",
    "l.33-.0": "S",
    "v-3.16": "T",
    "h.35": "U",
    "l-1.0": "V",
    "l-.69-3": "W",
    "v-1.52s": "Y",
    "v-.44s1": "Z",
}

def replace(line, repl, strg, location):
    """Replace graphics with correct character."""
    ret = re.sub(r' *<path class="cls-1201" d="M.{3,8},.{3,8}' + strg + '.*"/>', repl, line)
    if ret != line:
        mat = re.match(r'.*d="M([0-9.]{3,8}),([0-9.]{3,8}).*"', line)
        if location[0] == 0:
            location = [mat.group(1), mat.group(2)]
    return ret, location

def peaks(args):
    """
    Replace all peaks' graphical representation with names & elevation.
    """
    location = [0, 0]
    svg_out_file = open(args.outfile, 'w')
    oline = ''
    bline = '' # backup
    for line0 in open(args.infile, 'r'):
        bline += line0
        nline, location = replace(line0, "'", "v-.51s.", location)
        for key in LETTERS:
            nline, location = replace(nline, LETTERS[key], key, location)
        if nline != line0:
            oline += nline.rstrip()
            continue
        if len(oline) > 5:
            oline = '<circle class="" cx="' + location[0] + '" cy="' + location[1] + \
                '" data-name="PEAKN/' + oline + '"/>'
            print(oline, file=svg_out_file)
            print(nline, end='', file=svg_out_file)
        else:
            print(bline, end='', file=svg_out_file)

        bline = ''
        oline = ''
        location = [0, 0]

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
    args = parser.parse_args()
    peaks(args)

if __name__ == '__main__':
    main()
