#!/usr/bin/env python

'''Removes dependencies from protobufs files pulled in from etcd.
'''

import sys
import re

def strip_gogoproto(file_buf):
    return "\n".join(line for line in file_buf.split("\n") if 'gogoproto' not in line)

def strip_http_annotations(file_buf):
    # Removes the inside of like this:
    # { option (google.api.http) = {ANYTHING} }
    # Replacing it with:
    # { }
    file_buf = re.sub(r'\{\s*option\s*\(google.api\.http\)\s*=\s*\{.*?\};\s*\}', '{ }', file_buf, flags=re.DOTALL)
    # Remove import line
    return re.sub(r'^\s*import\s*"google/api/annotations.proto";\s*$', '', file_buf, flags=re.MULTILINE)

# Flatten the directory structure used in imports.
# e.g. import "etcd/foo/bar/baz.proto" becomes import "baz.proto"
def flatten_imports(file_buf):
    return re.sub(r'^\s*import\s*"etcd.*/(.*?.proto)";\s*$', r'import "\1";', file_buf, flags=re.MULTILINE)


with open(sys.argv[1], 'rw') as infile:
    print flatten_imports( strip_http_annotations( strip_gogoproto( infile.read())))
