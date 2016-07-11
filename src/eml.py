#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import sys
import os

import itertools
import collections

import datetime
import email_extract_json_unicode
import email
import uuid
import traceback
import json

sys.path.append("./utils")

from utils.file import spit, slurp, mkdirp, RollingFile

def timeNow():
    return datetime.datetime.now().strftime('%H:%M:%S')

def prn(msg):
    print "[{}] {}".format(timeNow(), msg)

def skip(iterable, at_start=0, at_end=0):
    it = iter(iterable)
    for x in itertools.islice(it, at_start):
        pass
    queue = collections.deque(itertools.islice(it, at_end))
    for x in it:
        queue.append(x)
        yield queue.popleft()

count_total = 0
def eml_files(dir_):
    global count_total
    for root, _, files in os.walk(dir_, followlinks=False):
        for filename in files:
#            filename, ext = os.path.splitext(filename)
#            if ext.replace(".","").lower() == "eml":
            if filename.endswith("_mime.txt") or filename.endswith(".eml"):
                count_total+=1
                print "Processing message: %s"%str(filename)
                yield os.path.abspath("{}/{}".format(root, filename))
            else:
                print "Skipping message: %s"%str(filename)

if __name__ == "__main__":

    desc = '''
examples:
    ./pst/eml.py {pst_emls_directory} output_path
    '''

    parser = argparse.ArgumentParser(
        description=" ... ", 
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("-l", "--limit", type=int, default=10, help="number of MB to limit output file size too, default 10MB")
    parser.add_argument("eml_root_path", help="root directory of .eml files")
    parser.add_argument("out_dir", help="ouput directory")
    parser.add_argument("-p", "--preserve_attachments", type=bool, default=False, help="Should inlined attachments be preserved as files or omitted from the results?  These are only the redundant attachments of the original message text, not named attachments.")

    parser.add_argument("-i", "--ingest_id", required=True, help="ingest id, usually the name of the email account, or the ingest process")
    parser.add_argument("-c", "--case_id", required=True, help="case id used to track and search accross multiple cases")
    parser.add_argument("-a", "--alt_ref_id", required=True, help="an alternate id used to corelate to external datasource")
    parser.add_argument("-b", "--label", required=True, help="user defined label for the dateset")


    #parser.add_argument("infile", nargs='?', type=argparse.FileType('r'), default=sys.stdin, help="Input File")
    args = parser.parse_args()
    emls_path = os.path.abspath(args.eml_root_path)

    count_failures = 0
    with RollingFile(args.out_dir, "part", args.limit) as outfile:
    
        for i, eml_file in enumerate(eml_files(emls_path)):
            guid = str(uuid.uuid1())
            try:
                categories = email_extract_json_unicode.categoryList(os.path.split(eml_file)[0].replace(emls_path, "", 1))
                message = email.message_from_string(slurp(eml_file))
                row = email_extract_json_unicode.extract(guid, message, categories, preserve_attachments=args.preserve_attachments)
                row["ingest_id"] = args.ingest_id
                row["case_id"] = args.case_id
                row["alt_ref_id"] = args.alt_ref_id
                row["label"] = args.label
                row["original_artifact"] = {"filename" : eml_file, "type" : "eml"}
                outfile.write(json.dumps(row) + "\n")
            except Exception as e:
                count_failures += 1
                traceback.print_exc()
                print "FAILED to process eml_file {}. Exception line: {} | {} ".format(eml_file, i, e.message)

            if i % 1000 == 0:
                prn("completed line: {}".format(i))

    print "Completed processing eml directories. Total messages={} Failures={}".format(count_total, count_failures)
