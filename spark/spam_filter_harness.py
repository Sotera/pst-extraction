import os
# import classifier, assumed in same dir
from spam_filter import NaiveBayesClassifier

import sys
import re
import argparse
import json
from functools import partial
from pyspark import SparkContext, SparkConf
import pickle
import dill

class RunSpamClassifier(object):
    def __init__(self):
        with open('spark/naive_bayes_classifier.pkl', 'rb') as input:
            self.NBC = dill.load(input)

    def run_classifier_part_files(self, doc_iter):
        class_id_spam = 'spam'
        class_id_nonspam = 'nonspam'

        for doc in doc_iter:
            doc_id = doc["id"]
            body = doc["body"]
            body = re.sub(r'[^\x00-\x7F]',' ', body)
            body = body.replace("[:newline:]", "           ")
            body = body.replace('\n','')
            body = body.replace('\t','')
            body = body.replace('\"','')
            body = body.replace('>','')
            body = body.replace('>>','')
            body = body.replace('<','')
            result = self.NBC.classify(body)
            if(result == class_id_nonspam):
                yield doc

def dump(x):
    return json.dumps(x)

if __name__ == '__main__':
    desc='Run spam classifier to filter spam.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    # Train naive bayes classifier:
    run_spam_classifier = RunSpamClassifier()
    parser.add_argument("input_emails_content_path", help="email part file")
    parser.add_argument("output_emails_with_entities", help="output directory for spark results spam emails filtered")
    args = parser.parse_args()
    conf = SparkConf().setAppName("Newman generate entities for emails")
    sc = SparkContext(conf=conf)
    rdd_emails = sc.textFile(args.input_emails_content_path).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(run_spam_classifier.run_classifier_part_files).map(dump).saveAsTextFile(args.output_emails_with_entities)
