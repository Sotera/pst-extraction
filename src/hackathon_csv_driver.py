#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import csv
import re

import argparse
import sys
import os
import json
import time
import mimetypes
import dateutil.parser
import dateutil.tz

import itertools
import collections

import datetime
import uuid
import traceback

sys.path.append("./utils")

from utils.file import slurpBase64, RollingFile


def timeNow():
    return datetime.datetime.now().strftime('%H:%M:%S')

def prn(msg):
    print "[{}] {}".format(timeNow(), msg)


def sanitize_field_names(name):
    return name.lower().replace(' ','_').replace(".","_")

email_regexp = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")

def csv_row_to_doc_iter(file):
    with open(file, 'rb') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(4096))
        csvfile.seek(0)
        has_header = csv.Sniffer().has_header(csvfile.read(4096))
        csvfile.seek(0)
        reader = csv.reader(csvfile, dialect)

        first_row = True
        metarow=''
        for row in reader:
            emailaddr = None
            for field in row:
                emailaddr = email_regexp.search(field)
                if emailaddr:
                    break;
            if emailaddr:
                yield (emailaddr.group(0), row, metarow)
            elif first_row and has_header:
                metarow = row
                print "Header row contains:"
                print ','.join(metarow)

# TODO implement me
def is_columnar(path):
    return True

count_total = 0
FILE_TYPES_BLACK_LIST=["mdb","msg","exe","zip","gz","dat"]
FILE_TYPES_WHITE_LIST=["csv"]


def guess_mime(filename):
    mime_type = mimetypes.guess_type(filename)[0]
    return mime_type if mime_type else "application/octet-stream"

def UTC_date(date_str):
    dt = dateutil.parser.parse(date_str)
    if not dt.tzinfo:
        dt =dt.replace(tzinfo=dateutil.tz.tzlocal())
        dt = dt.astimezone(dateutil.tz.tzutc())

        return dt.strftime('%Y-%m-%dT%H:%M:%S')


def crawl_files(root_dir, meta):
    global count_total
    _prefix_length = len(root_dir)
    for root, _, files in os.walk(root_dir):
        for filename in files:
            _, ext = os.path.splitext(filename)
            if ext.replace(".","").lower() in FILE_TYPES_BLACK_LIST:
                print "Skipping file: %s"%str(filename)
            else:
                count_total+=1
                print "Processing file: %s"%str(filename)
                abs_path = os.path.abspath("{}/{}".format(root, filename))
                (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(abs_path)
                # Max 100MB file size
                if size > 100000000:
                    print "Skipping large file: %s, size=%s"%(str(filename),str(size))
                    continue
                # filename, ext = os.path.splitext(file)
                rel_path = str(abs_path[(_prefix_length if not abs_path[_prefix_length]=='/' else _prefix_length+1):])
                print "-- abs_path: %s"%str(abs_path)
                print "-- rel_path: %s"%str(rel_path)

                meta["original_artifact"] = {"filename" : rel_path, "type" : "files"}
                # Map Columnar data
                if is_columnar(abs_path):
                    for tup in csv_row_to_doc_iter(abs_path):
                        guid = str(uuid.uuid1())
                        row = {
                            "id" : guid,
                            "senders" : [str(uid)],
                            "senders_line" : str(uid),
                            "tos" : [tup[0]],
                            "tos_line"  : tup[0],
                            "ccs":[],
                            "ccs_line": "",
                            "bccs":[],
                            "bccs_line": "",
                            "subject" : rel_path,
                            "body" : ','.join(tup[2]) + "\n" + ','.join(tup[1]),
                            "datetime" : UTC_date(time.ctime(mtime)),
                            "attachments" : [],
                            "extended" : {}
                        }
                        if tup[2]:
                            dictionary = dict(zip(map(lambda x: sanitize_field_names(x), tup[2]), tup[1]))
                            row["extended"].update(dictionary)
                        row.update(meta)

                        yield json.dumps(row)
                else:
                    raise Exception("File must be columnar.")

if __name__ == "__main__":

    desc = '''
examples:
    ./filecrawl.py {files_directory} output_path
    '''

    parser = argparse.ArgumentParser(
        description=" ... ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("-l", "--limit", type=int, default=10, help="number of MB to limit output file size too, default 10MB")
    parser.add_argument("file_root_path", help="root directory of files")
    parser.add_argument("out_dir", help="ouput directory")
    parser.add_argument("-i", "--ingest_id", required=True, help="ingest id, usually the name of the email account, or the ingest process")
    parser.add_argument("-c", "--case_id", required=True, help="case id used to track and search accross multiple cases")
    parser.add_argument("-a", "--alt_ref_id", required=True, help="an alternate id used to corelate to external datasource")
    parser.add_argument("-b", "--label", required=True, help="user defined label for the dateset")

    args = parser.parse_args()

    meta = {}
    meta["ingest_id"] = args.ingest_id
    meta["case_id"] = args.case_id
    meta["alt_ref_id"] = args.alt_ref_id
    meta["label"] = args.label


    files_path = os.path.abspath(args.file_root_path)

    with RollingFile(args.out_dir, "part", args.limit) as outfile:
        for i, crawl_file in enumerate(crawl_files(files_path, meta)):
            try:
                outfile.write( crawl_file + "\n")
            except Exception as e:
                traceback.print_exc()
                print "exception line: {} | {} ".format(i, e.message)

            if i % 1000 == 0:
                prn("completed line: {}".format(i))
    print "Total processed: {}".format(count_total)
