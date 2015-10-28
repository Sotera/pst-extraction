#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import json
from elasticsearch import Elasticsearch

def load_lda_map(lda_file, base_index):
    es = Elasticsearch()

    with open(lda_file) as f:
        clusters = [json.loads(line) for line in f.readlines()]
    print clusters
    for cluster in clusters:
        res = es.index(index=base_index, doc_type='lda-clustering', body=cluster)
        print(res['created'])

if __name__ == "__main__":
    desc='Ingest the lda-cluster map into elasticsearch.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("index", help="index name")

    args = parser.parse_args()

    load_lda_map("./tmp/lda.map.txt", args.index)
