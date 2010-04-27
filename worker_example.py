#!/usr/bin/env python
# encoding: utf-8

import sys
import hashlib

import shove

from utils import helper


def http_fetch(key, storage):
    """
    GET key -  of the HTML page
        storage - key-value storage
    RETURN storage key
    """
    url_key = "%s.url" % key
    html_key = "%s.html" % key

    url = storage[url_key]
    html = helper.fetchUrl(url).read()
    storage[html_key] = html
    return html_key

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s <storage> <obj_id> [<obj_id>, <obj_id>, ...]" % sys.argv[0]
        sys.exit(13)
    STORAGE = shove.Shove(sys.argv[1])
    for obj_id in sys.argv[2:]:
        print u"Working on %s" % obj_id
        key = http_fetch(obj_id, STORAGE)
        STORAGE.sync()
        print "Stored document with key", key

