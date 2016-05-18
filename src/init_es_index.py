#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import json
from elasticsearch import Elasticsearch

if __name__ == "__main__":
    desc='Ingest the lda-cluster map into elasticsearch.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("index", help="index name")
    parser.add_argument("--es_nodes", default="127.0.0.1:9200", help="es nodes")

    parser.add_argument("-i", "--ingest_id", required=True, help="ingest id, usually the name of the email account, or the ingest process")
    parser.add_argument("-c", "--case_id", required=True, help="case id used to track and search accross multiple cases")
    parser.add_argument("-a", "--alt_ref_id", required=True, help="an alternate id used to corelate to external datasource")
    parser.add_argument("-b", "--label", required=True, help="user defined label for the dateset")


    args = parser.parse_args()

    meta = {}
    meta["ingest_id"] = args.ingest_id
    meta["case_id"] = args.case_id
    meta["alt_ref_id"] = args.alt_ref_id
    meta["label"] = args.label

    nodes=[{"host":str(node), "port":9200} for node in args.es_nodes.split(',')]   

    print "es connecting to " + str(nodes)

    es = Elasticsearch(nodes)

    res = es.index(index=args.index, doc_type=args.ingest_id, id=args.ingest_id, body=meta)
