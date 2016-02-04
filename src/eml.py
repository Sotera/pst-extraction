#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import sys
import os

import itertools
import collections

import datetime
import email_extract_json
import email
import uuid
import traceback

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

count = 0
def eml_files(dir_):
    global count
    for root, _, files in os.walk(dir_):
        for filename in files:
#            filename, ext = os.path.splitext(filename)
#            if ext.replace(".","").lower() == "eml":
            if filename.endswith("_mime.txt") or filename.endswith(".eml"):
                count+=1
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
    #parser.add_argument("infile", nargs='?', type=argparse.FileType('r'), default=sys.stdin, help="Input File")
    args = parser.parse_args()
    emls_path = os.path.abspath(args.eml_root_path)

    with RollingFile(args.out_dir, "part", args.limit) as outfile:
    
        for i, eml_file in enumerate(eml_files(emls_path)):
            guid = str(uuid.uuid1())
            try:
                categories = email_extract_json.categoryList(os.path.split(eml_file)[0].replace(emls_path, "", 1))
                message = email.message_from_string(slurp(eml_file))
                row = email_extract_json.extract(guid, message, categories)
                outfile.write(row + "\n")
            except Exception as e:
                traceback.print_exc()        
                print "exception line: {} | {} ".format(i, e.message)

            if i % 1000 == 0:
                prn("completed line: {}".format(i))
    print "Total processed: {}".format(count) 
