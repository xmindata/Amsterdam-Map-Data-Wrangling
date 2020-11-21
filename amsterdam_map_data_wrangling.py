#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The process for the data transformation is as follows:
- Use iterparse to iteratively step through each top level element in the OSM XML
- Shape each element into several data structures using a custom function
- Write each data structure to the appropriate .csv files

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:
- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
- user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

"""
import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import pandas as pd
import sqlite3
import datetime
import os

OSM_PATH = "ams_s.osm"
NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"




LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements
    # YOUR CODE HERE
    if element.tag == 'node':
        for i in node_attr_fields:
            node_attribs[i] = element.attrib[i]
        for t in element.iter("tag"):
            temp = {}
            temp['id'] = element.attrib['id']
            temp['value'] = t.attrib['v']
            is_post = False

            m = re.search(LOWER_COLON, t.attrib['k'])

            if m>-1:
                colon_location = t.attrib['k'].find(":")
                temp['type'] = t.attrib['k'][:colon_location]
                temp['key'] = t.attrib['k'][colon_location +1:]

            else:
                temp['type'] = 'regular'
                temp['key'] = t.attrib['k']

            if temp['key'] == 'postcode':
                temp['value']=t.attrib['v'].lstrip()[0:4]+" "+ t.attrib['v'].rstrip()[-2:]

            elif t.attrib['k'] == 'phone':
                m = re.findall(r'([0-9]*)', t.attrib['v'])
                if m > -1:
                    phone_f = ''.join(m)
                    if len(phone_f) == 11 or len(phone_f) == 9:
                        temp['value'] = '+'+phone_f
                    elif len(phone_f) == 12:
                        temp['value'] = '+'+phone_f[:2] + phone_f[3:]
                    elif len(phone_f) == 10 or len(phone_f) == 8:
                        temp['value'] = '+31'+phone_f[1:]
                    elif len(phone_f) == 13:
                        temp['value'] = '+'+phone_f[2:]
                    elif len(phone_f) == 7:
                        temp['value'] = '+31'+phone_f
                    else:
                        temp['value'] = phone_f
            tags.append(temp)
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        for i in WAY_FIELDS:
            way_attribs[i] = element.attrib[i]
        position_count = 0
        for t in element.iter("nd"):
            temp_nd = {}
            temp_nd['id'] = element.attrib['id']
            temp_nd['node_id'] = t.attrib['ref']
            temp_nd["position"]= position_count
            position_count +=1
            way_nodes.append(temp_nd)
        for t in element.iter("tag"):
            temp_tg = {}
            temp_tg['id'] = element.attrib['id']
            temp_tg['value'] = t.attrib['v']

            m = re.search(LOWER_COLON, t.attrib['k'])
            if m>-1:
                colon_location = t.attrib['k'].find(":")
                temp_tg['type'] = t.attrib['k'][:colon_location ]
                temp_tg['key'] = t.attrib['k'][colon_location +1:]
            else:
                temp_tg['type'] = 'regular'
                temp_tg['key'] = t.attrib['k']
            tags.append(temp_tg)
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}

# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

def is_phone(elem):
    return (elem.attrib['k'] == 'phone')

class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

if __name__ == '__main__':

    a = datetime.datetime.now().replace(microsecond=0)
    process_map(OSM_PATH)
    b = datetime.datetime.now().replace(microsecond=0)
    print "Data processed... \n time spent: ", (b-a)

    for doc in [OSM_PATH, NODES_PATH , NODE_TAGS_PATH, WAYS_PATH, WAY_NODES_PATH, WAY_TAGS_PATH]:
        print doc, "...", os.path.getsize(doc)/1024/1024, "MB"
