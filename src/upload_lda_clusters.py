#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import json
from elasticsearch import Elasticsearch

def load_lda_map(nodes, lda_file, base_index):
    print "es connecting to " + str(nodes)

    es = Elasticsearch(nodes)

    with open(lda_file) as f:
        clusters = [json.loads(line) for line in f.readlines()]
    #print clusters
    for cluster in clusters:
        res = es.index(index=base_index, doc_type='lda-clustering', id=cluster["idx"], body=cluster)
        #print(res['created'])

if __name__ == "__main__":
    desc='Ingest the lda-cluster map into elasticsearch.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("index", help="index name")
    parser.add_argument("--es_nodes", default="127.0.0.1:9200", help="es nodes")
    
    args = parser.parse_args()
    
    nodes=[{"host":str(node), "port":9200} for node in args.es_nodes.split(',')]   
    load_lda_map(nodes, "./tmp/lda.map.txt", args.index)
