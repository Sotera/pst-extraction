#!/usr/bin/env python
from pyspark import SparkContext, SparkConf

import sys, os
import json
import argparse
from operator import itemgetter

def parseLDA(line):
    uid, scores = line.strip().split("\t")
    topic_scores = {"idx_{}".format(topic_idx):score
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
    args = parser.parse_args()
    conf = SparkConf().setAppName(desc)
    sc = SparkContext(conf=conf)
    
    rdd_emails = sc.textFile(args.input_path_emails).map(lambda x: json.loads(x)).keyBy(itemgetter('id'))
    rdd_lda = sc.textFile(args.input_path_lda).map(parseLDA)

    joined = rdd_emails.leftOuterJoin(rdd_lda).map(addTopicScores)

    joined.map(lambda x: json.dumps(x)).saveAsTextFile(args.output_path)
    
    print "complete."
