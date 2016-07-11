#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

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

def skip(iterable, at_start=0, at_end=0):
    it = iter(iterable)
    for x in itertools.islice(it, at_start):
        pass
    queue = collections.deque(itertools.islice(it, at_end))
    for x in it:
        queue.append(x)
        yield queue.popleft()

count_total = 0
FILE_TYPES_BLACK_LIST=["mdb","msg","exe","zip","gz","dat"]
FILE_TYPES_WHITE_LIST=[]


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
                print "Skipping message: %s"%str(filename)
            else:
                count+=1
                print "Processing message: %s"%str(filename)
                abs_path = os.path.abspath("{}/{}".format(root, filename))
                (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(abs_path)
                # filename, ext = os.path.splitext(file)
                guid = str(uuid.uuid1())
                rel_path = str(abs_path[(_prefix_length if not abs_path[_prefix_length]=='/' else _prefix_length+1):])
                print "-- abs_path: %s"%str(abs_path)
                print "-- rel_path: %s"%str(rel_path)

                meta["original_artifact"] = {"filename" : rel_path, "type" : "files"}
                row = {
                    "id" : guid,
                    "senders" : [str(uid)],
                    "senders_line" : str(uid),
                    "tos" : ["none"],
                    "tos_line"  : "none",
                    "ccs":[],
                    "ccs_line": "",
                    "bccs":[],
                    "bccs_line": "",
                    "subject" : rel_path,
                    "body" : "",
                    "datetime" : UTC_date(time.ctime(mtime)),
                    "attachments" : [
                        {
                            "guid" : str(uuid.uuid1()),
                            "contents64" : slurpBase64(abs_path),
                            "filename" : filename,
                            "extension" :ext.replace(".","").lower(),
                            "content_type" : guess_mime(filename),
                            "filesize": size,
                            "created" : UTC_date(time.ctime(ctime)),
                            "modified": UTC_date(time.ctime(mtime)),
                        }
                    ]}
                row.update(meta)

                yield json.dumps(row)

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
