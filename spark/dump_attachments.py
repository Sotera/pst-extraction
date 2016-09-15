import os
import base64

import argparse
import json
from pyspark import SparkContext, SparkConf


def dump(x):
    return json.dumps(x)

def foo(email):
    print email

def process_email_attachments(path, email):
    for attachment in email["attachments"]:
        if "contents64" in attachment:
            attch_data = str(base64.b64decode(attachment["contents64"]))

            filename = os.path.join(path, attachment["filename"])
            print "Writing file: "+ filename
            with open(filename, 'wb') as outfile:
                outfile.write(attch_data)
    return email

if __name__ == '__main__':
    desc='Dump attachment binaries to local file system storage'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    # SPARK
    #
    parser.add_argument("input_normalized_json_path", help="input attachments")
    parser.add_argument("output_attachments_path", help="output attachments to lfs", default="tmp/attachments")
    args = parser.parse_args()

    conf = SparkConf().setAppName("Attachment dumper")

    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_normalized_json_path).map(lambda x: json.loads(x))

    # Can add coalesce to reduce to one machine if it has the HD space
    rdd_emails.foreach(lambda email : process_email_attachments(args.output_attachments_path, email))
