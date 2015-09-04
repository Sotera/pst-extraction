#!/usr/bin/env python
from pyspark import SparkContext, SparkConf

import sys, os
import json
import argparse

def rmkey(k, o):
    if k in o:
        del o[k]
    return o

def extractKeys(keys, o):
    rtn = {}
    for k in keys:
        if k in o:
            rtn[k] = o[k]
    return rtn
            
def removeAttachments(x):
    x['attachments'] = map(lambda o: rmkey('contents64', o), x['attachments'])
    return x

def extractAttachments(x):
    message_id = x['id']
    attachments = map(lambda o: extractKeys(['guid', 'extension', 'filename', 'contents64'], o), x['attachments'])
    attachments = [dict(a, **{'id': message_id }) for a in attachments]
    return attachments

def dump(x):
    return json.dumps(x)

if __name__ == "__main__":

    desc='newman split emails and attachment for indexing '
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("input_emails_content_path", help="joined email extracted content and base64 attachment")
    parser.add_argument("output_path_emails", help="output directory for spark results emails without base64 attachment")
    parser.add_argument("output_path_raw_attachments", help="output directory for spark results attachments ")
    
    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman split attachments and emails")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_emails_content_path).map(lambda x: json.loads(x))
    rdd_emails.map(removeAttachments).map(dump).saveAsTextFile(args.output_path_emails)
    rdd_emails.flatMap(extractAttachments).map(dump).saveAsTextFile(args.output_path_raw_attachments)    

