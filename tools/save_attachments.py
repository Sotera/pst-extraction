#!/usr/bin/env python
import argparse

import base64
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from functools import partial

def getIn(d, arraypath, default=None):
    if not d:
        return d 
    if not arraypath:
        return d
    else:
        return getIn(d.get(arraypath[0]), arraypath[1:], default) \
            if d.get(arraypath[0]) else default

def getAttachment(es, index, uuid):
    query = {"index": index,
             "doc_type": "attachments",
             "id": uuid }
    try: 
        return (True, es.get(**query))
    except NotFoundError:
        return (False, None)

def saveAttachment(output_dir, optional_json):
    successful, o = optional_json
    if not successful:
        return

    filename = getIn(o, ["_source", "filename"], "")
    content64 = getIn(o, ["_source", "contents64"], "")
    _bytes = base64.b64decode(content64)
    print u"save " + filename 
    with open("{}/{}".format(output_dir, filename), "wb") as f:
        f.write(_bytes)

if __name__ == "__main__":
    desc = "pull attachments from ES instance and save to disk"
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("--es_host", default="localhost:9200", help="es host string HOST:PORT")
    parser.add_argument("--es_index", default="sample", help="index name")    
    parser.add_argument("-o", "--output_dir", default=".", help="dir to save attachments")
    parser.add_argument('attachment_ids', metavar='attachment_id', nargs='+', help='var arg of attachment uuids')
    args = parser.parse_args()
    es = Elasticsearch([args.es_host])
    get = partial(getAttachment, es, args.es_index)
    save = partial(saveAttachment, args.output_dir)
    process = lambda x : save(get(x))

    for attach_id in args.attachment_ids:
        process(attach_id)

