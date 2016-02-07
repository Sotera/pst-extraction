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

count = 0
FILE_TYPES_BLACK_LIST=[]
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

def crawl_files(dir_):
    global count
    for root, _, files in os.walk(dir_):
        for filename in files:
            _, ext = os.path.splitext(filename)
            if ext.replace(".","").lower() not in FILE_TYPES_BLACK_LIST:
                count+=1
                print "Processing message: %s"%str(filename)
                abs_path = os.path.abspath("{}/{}".format(root, filename))
                (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(abs_path)
                # filename, ext = os.path.splitext(file)
                guid = str(uuid.uuid1())
                yield json.dumps({
                    "guid" : guid,
                    "id" : guid,
                    "senders" : str(uid),
                    "senders_line" : str(uid),
                    "datetime" : UTC_date(time.ctime(mtime)),
                    "attachments" : [
                        {
                            "guid" : str(uuid.uuid1()),
                            "contents64" : slurpBase64(abs_path),
                            "filename" : filename,
                            "extension" :ext.replace(".","").lower(),
                            "mime" : guess_mime(filename),
                            "filesize": size,
                            "created" : UTC_date(time.ctime(ctime)),
                            "modified": UTC_date(time.ctime(mtime)),
                        }
                    ]})
            else:
                print "Skipping message: %s"%str(filename)

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
    args = parser.parse_args()
    files_path = os.path.abspath(args.file_root_path)

    with RollingFile(args.out_dir, "part", args.limit) as outfile:
        for i, crawl_file in enumerate(crawl_files(files_path)):
            try:
                outfile.write( crawl_file + "\n")
            except Exception as e:
                traceback.print_exc()
                print "exception line: {} | {} ".format(i, e.message)

            if i % 1000 == 0:
                prn("completed line: {}".format(i))
    print "Total processed: {}".format(count) 
