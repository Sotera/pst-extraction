#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import sys
import os

import itertools
import collections

import datetime
import email_extract_json
import mailbox
import uuid
import traceback

sys.path.append("./utils")

from utils.file import spit, slurp, mkdirp

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

def mbox_files(dir_):
    for root, _, files in os.walk(dir_):
        for f in files:
            if f == "mbox":
                yield os.path.abspath("{}/{}".format(root, f))


if __name__ == "__main__":

    desc = '''
examples:
    ./pst/mbox.py {pst_mbox_directory} output_path
    '''

    parser = argparse.ArgumentParser(
        description=" ... ", 
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    #parser.add_argument("-a","--header", action='store_true', help="add header to output")
    #parser.add_argument("-s","--start", type=int, default=0, help="start at line #")
    #parser.add_argument("-l", "--limit", type=int, default=0, help="end at line #")
    parser.add_argument("mbox_path", help="mbox file path")
    parser.add_argument("out_dir", help="ouput directory")
    #parser.add_argument("infile", nargs='?', type=argparse.FileType('r'), default=sys.stdin, help="Input File")
    args = parser.parse_args()

    mbox_path = os.path.abspath(args.mbox_path)
    for i, mbox_file in enumerate(mbox_files(mbox_path)):
        outfile = "{}/output_part_{:06d}".format(args.out_dir, i)
        print mbox_file
        
        for j, message in enumerate(mailbox.mbox(mbox_file)):
            guid = str(uuid.uuid1())
            try:
                categories = email_extract_json.categoryList(os.path.split(mbox_file)[0].replace(mbox_path, "", 1))
                row = email_extract_json.extract(guid, message, categories)
                spit(outfile, row + "\n")
            except Exception as e:
                traceback.print_exc()        
                print "exception line: {} | {} ".format(j, e.message)

            if j % 100 == 0:
                prn("completed line: {}".format(j)) 
