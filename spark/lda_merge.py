#!/usr/bin/env python
from pyspark import SparkContext, SparkConf

import sys, os
import json
import argparse
import datetime

from operator import itemgetter
from functools import partial
from filters import valid_json_filter

def parseLDA(line):
    uid, scores = line.strip().split("\t")
    topic_scores = {"idx_{}".format(topic_idx):float(score)
                    for topic_idx, score in enumerate(scores.split())}
    return (uid, topic_scores)

def addTopicScores(t):
    (id_, (email, scores)) = t
    return dict(email, topic_scores=scores)
    

if __name__ == "__main__":

    desc='newman attach lda to documents'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("input_path_emails", help="directory with json emails")
    parser.add_argument("input_path_lda", help="directory with lda results")
    parser.add_argument("output_path", help="output directory for joined emails")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()
    conf = SparkConf().setAppName(desc)
    sc = SparkContext(conf=conf)

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)


    rdd_emails = sc.textFile(args.input_path_emails).filter(filter_fn).map(lambda x: json.loads(x)).keyBy(itemgetter('id'))
    rdd_lda = sc.textFile(args.input_path_lda).map(parseLDA)

    joined = rdd_emails.leftOuterJoin(rdd_lda).map(addTopicScores)

    joined.map(lambda x: json.dumps(x)).saveAsTextFile(args.output_path)
    
    print "complete."
