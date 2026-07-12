#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Extract additional information.
"""
from dataclasses import dataclass
import re
import os

@dataclass
class Buffer:
    """Encapsulate buffer."""
    loc: list
    txt: str = ''
    lines: str = ''

LETTERS = {
    "v-.51s.":                         "'",
    "c0-.6.0":                         "0",
    "h-.33":                           "1",
    "v.42":                            "2",
    "l.32-.0":                         "3",
    "v-.86s-":                         "4",
    "l.34-.0":                         "5",
    "l-.32.0":                         "6",
    "v-.42[sh]1.7":                    "7",
    "c-.13-.0":                        "8",
    "l.31-.04c":                       "9",
    "l1.0":                            "A",
    "v-3.58s1,0,1,0c.2,0,.36.04.48.1": "B",
    "l.35.12":                         "C",
    "v-3.58s.92,0,.92,0c.2":           "D",
    "v-3.58s1.92,0,1.92,0":            "E",
    "v-3.58s1.79":                     "F",
    "v-.42[hs]1.1":                    "G",
    "v-3.58s.36,0,.36,0v1.47s":        "H",
    "v-3.58s.36,0,.36,0v3.58s":        "I",
    "v-3.58h.35v3.58h-.35Z":           "I",
    "l.31-.06c0":                      "J",
    "v-3.58s.36,0,.36,0v1.7":          "K",
    "v-3.58h.35v1":                    "K",
    "v-3.58s.36,0,.36,0v3.16s":        "L",
    "v-3.58s.54":                      "M",
    "v-3.58s.37":                      "N",
    "v-3.58h.36":                      "N",
    "c0-.61.":                         "O",
    "v-3.58s1.01":                     "P",
    "c.16.15":                         "Q",
    "v-3.58[hs]1.1":                   "R",
    "l.33-.0":                         "S",
    "v-3.16":                          "T",
    "h.35":                            "U",
    "l-1.0":                           "V",
    "l-.69-3":                         "W",
    "v-1.52s":                         "Y",
    "v-.44s1":                         "Z"
}

def graph2char(line, repl, strg, location):
    """Replace graphics with correct character."""
    ret = re.sub(r' *<path class="cls-1201" d="M.{3,8},.{3,8}' + strg + '.*"/>', repl, line)
    if ret != line:
        mat = re.match(r'.*d="M([0-9.]{3,8}),([0-9.]{3,8}).*"', line)
        if location[0] == 0:
            location = [mat.group(1), mat.group(2)]
    return ret, location

def substitute_spans(infile, outpre):
    """Replace all <span/> names."""
    svg_infile = open(infile, 'r')
    svg_outfile = open(outpre + ".svg.tmp", 'w')
    buf = Buffer(loc=[0, 0])
    for new_line in svg_infile:
        new_txt = new_line
        buf.lines += new_line

        # Graphical peak text
        for key in LETTERS:
            new_txt, buf.loc = graph2char(new_txt, LETTERS[key], key, buf.loc)
        if new_txt != new_line:
            buf.txt += new_txt.rstrip()
            continue
        if len(buf.txt) > 5:
            new_line = f'<circle class="" cx="{buf.loc[0]}" cy="{buf.loc[1]}" ' + \
                f'data-name="PeakName/{buf.txt}"/>\n' + new_txt
            print(new_line, end='', file=svg_outfile)
            buf = Buffer(loc=[0, 0])
            continue

        # Real text
        new_txt = re.sub(r'<tspan [^>]*>([a-z A-Z0-9\-\(’\)]+)</tspan>', '\\1', buf.lines)
        # - rare occasion of tspan in tspan
        new_txt = re.sub(r'<tspan [^>]*>([a-z A-Z0-9\-\(’\)]+)</tspan>', '\\1', new_txt).rstrip()
        new_txt = re.sub(
            r' *<text .*(cls-[0-9]*).*translate\(([0-9.]{3,8}) ([0-9.]{3,8})\).*>' + \
            r'([a-z A-Z0-9\-\(’\)]+)</text>',
            '<circle class="\\1" cx="\\2" cy="\\3" data-name="AnyName/\\4"/>',
            new_txt
        )
        if new_txt != buf.lines.rstrip():
            print(new_txt, end='', file=svg_outfile)
            print('', file=svg_outfile) # CR
            buf = Buffer(loc=[0, 0])
            continue

        print(buf.lines, end='', file=svg_outfile)
        buf = Buffer(loc=[0, 0])

    svg_infile.close()
    svg_outfile.close()
    os.rename(outpre + ".svg.tmp", outpre + ".svg")
    return outpre + ".svg"

def substitute_circles(infile, outpre):
    """Connect circle Names that have y-distance of 6."""
    svg_infile = open(infile, 'r')
    svg_outfile = open(outpre + ".svg.tmp", 'w')
    bline = ''
    og_1 = ''
    og_2 = ''
    og_3 = ''
    og_4 = ''
    for line0 in svg_infile:
        mat = re.match(
            r'<circle class="cls-(.*)" cx="(.*)" cy="(.*)" data-name="AnyName/(.*)"/>', line0
        )
        if mat is not None:
            [g_1, g_2, g_3, g_4] = [mat.group(1), mat.group(2), mat.group(3), mat.group(4)]
            if og_1 == g_1 and int(10*(float(g_3) - float(og_3))) == 60:
                g_4 = og_4.strip() + g_4.strip()
                print(
                    f'<circle class="" cx="{og_2}" cy="{og_3}" data-name="AnyName/{g_4}"/>',
                    end='\n', file=svg_outfile
                )
                bline = ''
                og_1 = ''
            else:
                print(bline, end='', file=svg_outfile)
                [og_1, og_2, og_3, og_4] = [g_1, g_2, g_3, g_4]
                bline = line0
        else:
            print(bline, end='', file=svg_outfile)
            print(line0, end='', file=svg_outfile)
            bline = ''
            og_1 = ''

    svg_infile.close()
    svg_outfile.close()
    os.rename(outpre + ".svg.tmp", outpre + ".svg")
    return outpre + ".svg"
