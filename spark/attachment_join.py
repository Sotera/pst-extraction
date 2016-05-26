#!/usr/bin/env python
from pyspark import SparkContext, SparkConf

import sys, os
import json
import argparse

def findIdx(pred, l):
    for pos, o in enumerate(l):
        if pred(o):
            return (True, pos)
    return (False, None)

def fn_attach_array(x):
    id_, attachs = x.split("\t")
    attachments = json.loads(attachs)
    return {'id' : id_, 'attachments': attachments }

# add extracted contents to json email
# (K, (V, U))
def fn_join_contents(x):
    k, v = x
    email_json, attach_obj = v
    attach_obj = attach_obj if attach_obj else {'id': "", 'attachments': []}
    for attach in attach_obj['attachments']:
        success, idx = findIdx(lambda x: x['guid'] == attach['guid'], email_json['attachments'])
        if success:
            if 'content' in attach:
                email_json['attachments'][idx]['content'] = attach['content']
            if 'image_analytics' in attach:
                if 'image_analytics' in email_json['attachments'][idx]:
                    email_json['attachments'][idx]['image_analytics'].update(attach['image_analytics'])
                else:
                    email_json['attachments'][idx]['image_analytics'] = attach['image_analytics']
            if 'content_encrypted' in attach:
                email_json['attachments'][idx]['content_encrypted'] = attach['content_encrypted']
            if 'content_extracted' in attach:
                email_json['attachments'][idx]['content_extracted'] = attach['content_extracted']
    return json.dumps(email_json)

if __name__ == "__main__":

    desc='newman join attachment contents '
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("input_emails_path", help="raw email json data")
    parser.add_argument("attach_input", help="extracted contents:  comma delimitted list of paths, can be tika, ocr, etc")

    parser.add_argument("output_path", help="output directory for spark results")

    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman join attachments content")
    sc = SparkContext(conf=conf)

    # old
    # rdd_extracted_content = sc.textFile(args.attach_input).map(fn_attach_array).keyBy(lambda x: x['id'])
    # rdd_emails = sc.textFile(args.input_emails_path).map(lambda x: json.loads(x)).keyBy(lambda x: x['id'])
    # rdd_joined = rdd_emails.leftOuterJoin(rdd_extracted_content).map(fn_join_contents)
    # rdd_joined.saveAsTextFile(args.output_path)


    rdd_emails = sc.textFile(args.input_emails_path).map(lambda x: json.loads(x)).keyBy(lambda x: x['id'])

    # Join each of the content rdds to the email rdd
    # TODO fix this iteration - maybe key field is not correct once it joins?
    for input_path in args.attach_input.split(","):
        print "===============================joining datasets: {}".format(input_path)
        rdd_extracted_content = sc.textFile(input_path).map(fn_attach_array).keyBy(lambda x: x['id'])
        rdd_joined = rdd_emails.leftOuterJoin(rdd_extracted_content).map(fn_join_contents)

        # rdd_extracted_content2 = sc.textFile("pst-extract/ocr_output/").map(fn_attach_array).keyBy(lambda x: x['id'])
        # rdd_joined2 = rdd_joined.leftOuterJoin(rdd_extracted_content2).map(fn_join_contents)

    rdd_joined.saveAsTextFile(args.output_path)

