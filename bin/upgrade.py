#!/usr/bin/env python
#
# Usage: 
# upgrade.py with no arguments will increase the minor version 
# in every pom.xml file.
#
# With one argument it will change the version to that in every pom.xml file
# in the current project.
# 
# Additionally the property, <shade_append> will be set to a packagized version 
# of the version string. 


from lxml import etree
from glob import glob
import os
import sys
import re


NAMESPACE = 'http://maven.apache.org/POM/4.0.0'
NAMESPACES = {'xmlns': NAMESPACE}
PROJECT_DIRECTORY = os.path.join(os.path.dirname(__file__), '..')
PARENT_POM_FILE = os.path.join(PROJECT_DIRECTORY, 'pom.xml')
SHADE_APPEND_PROPERTY = 'shade_append'

def usage():
    print("%s <version>" % (sys.argv[0], ))
    print("Version is a maven version string of format: <n>.<n>.<n>[-<suffix>]")
    sys.exit(1)

def update_cell(xml, xpath, value):
    res = xml.xpath(xpath, namespaces=NAMESPACES)
    res[0].text = value

def parse_version(version):
    match = re.match('^(\d+)\.(\d+)\.(\d+)((?:-.*)|)$', version)
    if not match:
        raise ValueError("Could not parse version: %s, expected format: <n>.<n>.<n>[-<suffix>]", 
                         version)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)), match.group(4))


parent_pom = etree.parse(PARENT_POM_FILE)
version = parent_pom.xpath('/xmlns:project/xmlns:version/text()', namespaces=NAMESPACES)
version = version[0]

if len(sys.argv) > 2:
    usage()
elif len(sys.argv) == 2:
    new_version = sys.argv[1]
    parsed_new_version = parse_version(new_version)
else:
    old_version = parse_version(version)
    parsed_new_version = (old_version[0], old_version[1], old_version[2] + 1, old_version[3])

new_version_string = '%d.%d.%d%s' % parsed_new_version
shade_version_string = 'v%d_%d_%d%s' % (parsed_new_version[0], 
                                        parsed_new_version[1], 
                                        parsed_new_version[2], 
                                        re.sub('[^a-zA-Z0-9_]', '_', parsed_new_version[3]))

update_cell(parent_pom, '/xmlns:project/xmlns:version', new_version_string)
update_cell(parent_pom, 
            '/xmlns:project/xmlns:properties/xmlns:%s' % (SHADE_APPEND_PROPERTY, ), 
            shade_version_string)

modules = parent_pom.xpath('/xmlns:project/xmlns:modules/xmlns:module/text()',
                           namespaces=NAMESPACES)

for module in modules:
    pom = os.path.join(PROJECT_DIRECTORY, module, 'pom.xml')
    pom_xml = etree.parse(pom)
    update_cell(pom_xml, '/xmlns:project/xmlns:parent/xmlns:version', new_version_string)
    pom_xml.write(pom)


parent_pom.write(PARENT_POM_FILE)

print("Upgraded to version: %s" % (new_version_string, ))
