#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import datetime
import codecs
import json
"""
Copied over from Lesson_6_Case_Study_OSM/12-Preparing_for_Database and modified slightly
"""

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

def is_float(string):
    try:
        float(string)
        return not string.startswith("0")
    except ValueError:
        return False

def is_int(string):
    try:
        int(string)
        return not string.startswith("0")
    except ValueError:
        return False

def parse_type(value):
    if is_int(value):
        return int(value)
    elif is_float(value):
        return float(value)
    else:
        return value

def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        node["type"] = element.tag
        for k in element.attrib:
            v = element.attrib[k]
            if k in CREATED:
                if "created" not in node.keys():
                    node["created"] = {}
                node["created"][k] = parse_type(v)
            elif k in ["lat", "lon"]:
                if "pos" not in node.keys():
                    node["pos"] = [None, None]
                if k == "lat":
                    node["pos"][0] = float(v)
                else:
                    node["pos"][1] = float(v)
            elif problemchars.search(k) is None:
                node[k] = parse_type(v)
        for e in element.iter():
            if e.tag == "tag" and "k" in e.attrib.keys() and "v" in e.attrib.keys():
                k = e.attrib["k"]
                kk = k.split(":")
                v = e.attrib["v"]
                if (problemchars.search(k) is None) and (len(kk) <= 2):
                    if kk[0] == "addr" and len(kk) == 2:
                        if "address" not in node.keys():
                            node["address"] = {}
                        node["address"][kk[1]] = parse_type(v)
                    elif len(kk) == 1:
                        node[k] = parse_type(v)
            elif e.tag == "nd" and "ref" in e.attrib.keys():
                if "node_refs" not in node.keys():
                    node["node_refs"] = []
                node["node_refs"].append(e.attrib["ref"])
        return node
    else:
        return None


def process_map(file_in, file_out, pretty = False):
    # You do not need to change this file
    #data = []
    counter = 0
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                #data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
            counter += 1
            if counter % 100000 == 0:
                print ".",
    print
    return counter

if __name__ == "__main__":
    test()