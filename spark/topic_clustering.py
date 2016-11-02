#!/usr/bin/env python
from pyspark import SparkContext, SparkConf

import sys, os, re
import json
import argparse

import codecs
import datetime

from collections import Counter
from operator import add
from functools import partial
from filters import valid_json_filter

def juxt_any(predicates, obj):
    '''
    returns immediately on first True
    returns False if no predicates passn
    '''
    def genfn():
        for p in predicates:
            yield p(obj)
    for x in genfn():
        if x:
            return True
    return False


def dumps(o):
    return json.dumps(o)

def slurpA(fp):
    with open(fp) as x: data = x.read().splitlines()
    return data

def spit(filePath, data, overwrite=False):
        # write all contents to a file
        mode= 'w' if overwrite else 'a'
        with codecs.open(filePath, mode, 'utf-8') as x: x.write(data)

def doc_word_counts(sw, doc_tuple):
    doc_id, text = doc_tuple

    #prefilter conditions for lines
    predicates = [partial(re.search, x)
                  for x in ['^from:', '^to:', '^subject', '^sent:', '^cc']]
    prefilter = lambda x :  "" if juxt_any(predicates, x) else x

    line_words = [prefilter(line).split() for line in text.lower().split('\n')]
    
    # return true if word should be ignored
    def word_filtered(x):
        if len(x) < 4: return True
        return x in sw.value
    
    filtered_words = [word
                      for line in line_words
                      # flatten
                      for word in line
                      if not word_filtered(word)]
    
    # return word count per document
    return (doc_id, list(Counter(filtered_words).iteritems()))

if __name__ == "__main__":

    desc='newman topic clustering'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("input_path_emails", help="directory with json emails")
    parser.add_argument("--stopwords", default="etc/english.stopwords", help="stop words file")
    parser.add_argument("--vocab_index", default="tmp/vocab.idx", help="index of vocab")
    parser.add_argument("output_path", help="output directory for topic clustering")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()
    conf = SparkConf().setAppName(desc)
    sc = SparkContext(conf=conf)
    rdd = sc.textFile(args.input_path_emails)
    sw = slurpA(args.stopwords)
    broadcastStopwords=sc.broadcast(sw)

    def doc_to_tuple(sz):
        j = json.loads(sz)
        return (j.get('id'), j.get('body'))

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)

    doc_wcs = rdd.filter(filter_fn).map(doc_to_tuple).coalesce(50) \
                 .map(partial(doc_word_counts, broadcastStopwords)).cache()

    vocab = doc_wcs.flatMap(lambda x: x[1]) \
                   .reduceByKey(add) \
                   .map(lambda x: (x[1], x[0])) \
                   .sortByKey(False) \
                   .take(2000)

    vocab= [word for count_total, word in vocab]
    vocabIdx = {k:v for v,k in enumerate(vocab)}

    spit(args.vocab_index, u"\n".join([u"{}\t{}".format(v,k) for k,v in vocabIdx.iteritems()]))
    
    broadcastVocabLookup = sc.broadcast(set(vocab))
    broadcastVocabIdx = sc.broadcast(vocabIdx)

    def to_vector(tuple_):
        doc_id, term_counts = tuple_
        filtered_terms = [(broadcastVocabIdx.value[val], count)
                          for val,count in term_counts
                          if val in broadcastVocabLookup.value]
        
        dtv = [0] * len(broadcastVocabLookup.value)
        
        for k,v in filtered_terms:
            dtv[k] = v

        return (doc_id, dtv)

    output = doc_wcs.map(to_vector).map(lambda x: "{}\t{}".format(x[0]," ".join(map(str,x[1]))))
    output.saveAsTextFile(args.output_path) 
    
    print "complete."
