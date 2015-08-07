#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import sys

import itertools
import collections

import datetime
import email_extract
import mailbox
import uuid

sys.path.append("./demail")

from newman.utils.file import spit, slurp, mkdirp

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


if __name__ == "__main__":

    desc = '''
examples:
    ./pst/mbox.py {mbox_path}
    '''

    parser = argparse.ArgumentParser(
        description=" ... ", 
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("-a","--header", action='store_true', help="add header to output")
    #parser.add_argument("-s","--start", type=int, default=0, help="start at line #")
    #parser.add_argument("-l", "--limit", type=int, default=0, help="end at line #")
    parser.add_argument("mbox_path", help="mbox file path")
    parser.add_argument("out_dir", help="ouput directory")
    #parser.add_argument("infile", nargs='?', type=argparse.FileType('r'), default=sys.stdin, help="Input File")
    args = parser.parse_args()
    outfile = "{}/output.csv".format(args.out_dir)
    mkdirp("{}/emails".format(args.out_dir))

    for i, message in enumerate(mailbox.mbox(args.mbox_path)):
        guid = str(uuid.uuid1())
        try:
            row = email_extract.extract(guid, message, args.out_dir)
            spit(outfile, row + "\n")
        except Exception as e:
            print "exception line: {} | {} ".format(i, e.message)

        if i % 100 == 0:
            prn("completed line: {}".format(i)) 
