import os
import json
import argparse
import datetime
from filters import valid_json_filter
from functools import partial
from pyspark import SparkContext, SparkConf


def findIdx(pred, l):
    for pos, o in enumerate(l):
        if pred(o):
            return (True, pos)
    return (False, None)

def fn_attach_array(filter_fn, x):
    id_, attachs = x.split("\t")
    filter_fn(attachs)
    attachments = json.loads(attachs)
    return {'id' : id_, 'attachments': attachments }

def fn_join_contents(x, docex_mode=False):
    '''
    add extracted contents to json email (K, (V, U))
    :param x:
    :param docex_mode: if running as docex copy extracted text back to the body for processing
    :return:
    '''
    k, v = x
    email_json, attach_obj = v
    attach_obj = attach_obj if attach_obj else {'id': "", 'attachments': []}
    for src_attach in attach_obj['attachments']:
        success, idx = findIdx(lambda x: x['guid'] == src_attach['guid'], email_json['attachments'])
        if success:
            dest_attach = email_json['attachments'][idx]

            dest_attach.update(src_attach)

            # In Docex mode copy the content into the body - docex always 1 doc per email
            if docex_mode:
                if 'content' in dest_attach:
                        email_json['body'] = dest_attach['content']
                if 'image_analytics' in dest_attach and 'ocr_output' in dest_attach['image_analytics']:
                        email_json['body'] = dest_attach['image_analytics']['ocr_output']

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
    parser.add_argument("-d", "--docex_mode", action="store_true", help="docex mode copies extracted text to the email body for further analysis.  Only use with docex.  This will overwrite the email body!")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()
    print "INFO: docex_mode ",args.docex_mode

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "INFO: Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)

    conf = SparkConf().setAppName("Newman join attachments content")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_emails_path).filter(filter_fn).map(lambda x: json.loads(x)).keyBy(lambda x: x['id'])

    # Join each of the content rdds to the email rdd
    # TODO fix this iteration - maybe key field is not correct once it joins?
    for input_path in args.attach_input.split(","):
        print "===============================joining datasets: {}".format(input_path)
        rdd_extracted_content = sc.textFile(input_path).map(lambda doc : fn_attach_array(filter_fn, doc)).keyBy(lambda x: x['id'])
        rdd_joined = rdd_emails.leftOuterJoin(rdd_extracted_content).map(lambda x: fn_join_contents(x, args.docex_mode))

        # rdd_extracted_content2 = sc.textFile("pst-extract/ocr_output/").map(fn_attach_array).keyBy(lambda x: x['id'])
        # rdd_joined2 = rdd_joined.leftOuterJoin(rdd_extracted_content2).map(fn_join_contents)

    rdd_joined.saveAsTextFile(args.output_path)

