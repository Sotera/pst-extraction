#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import sys
import os

import itertools
import collections

import datetime
import uuid
import traceback
import json

from patent_xl_mapper import rec_to_row

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

def xl_files(dir_):
    for root, _, files in os.walk(dir_):
        for f in files:
            if f.endswith("xl"):
                yield os.path.abspath("{}/{}".format(root, f))
            else:
                print "{} is not an .xl (line del xml) file -- If you think it should be indexed pleases rename".format(f)

def json_transform(xml_doc):
    xml_doc.update({
        "attachments" : [],
        "bccs": [],
        "bccs_line": [],
        "body": xml_doc.get("abstract"),
        "categories": [],
        "ccs": [],
        "ccs_line": [],
        "datetime": xml_doc["pub_date"],
        "senders": [xml_doc.get("parent")] if xml_doc.get("parent") else [],
        "senders_line": [xml_doc.get("parent")] if xml_doc.get("parent") else [],
        "subject": xml_doc.get("title"),
        "tos": [xml_doc.get("pub_data")] if xml_doc.get("pub_data") else [],
        "tos_line": [xml_doc.get("pub_data")] if xml_doc.get("pub_data") else []
    })
    return xml_doc

if __name__ == "__main__":

    desc = '''
examples:
    ./pst/uspto_xml_reader.py {pst_xml_directory} output_path
    '''

    parser = argparse.ArgumentParser(
        description=" ... ", 
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    #parser.add_argument("-a","--header", action='store_true', help="add header to output")
    #parser.add_argument("-s","--start", type=int, default=0, help="start at line #")
    #parser.add_argument("-l", "--limit", type=int, default=0, help="end at line #")

    parser.add_argument("xl_path", help="xl file path - line del xml")
    parser.add_argument("out_dir", help="ouput directory")
    parser.add_argument("-p", "--preserve_attachments", type=bool, default=False, help="Should inlined attachments be preserved as files or omitted from the results?")

    parser.add_argument("-i", "--ingest_id", required=True, help="ingest id, usually the name of the email account, or the ingest process")
    parser.add_argument("-c", "--case_id", required=True, help="case id used to track and search accross multiple cases")
    parser.add_argument("-a", "--alt_ref_id", required=True, help="an alternate id used to corelate to external datasource")
    parser.add_argument("-b", "--label", required=True, help="user defined label for the dateset")

    #parser.add_argument("infile", nargs='?', type=argparse.FileType('r'), default=sys.stdin, help="Input File")
    args = parser.parse_args()

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    xl_path = os.path.abspath(args.xl_path)
    for i, xl_file in enumerate(xl_files(xl_path)):
        count_failures = 0
        outfile = "{}/output_part_{:06d}".format(args.out_dir, i)
        print xl_file

        with open(xl_file) as xl_fobj:
            for j, xml in enumerate([line.rstrip('\n') for line in xl_fobj]):
                # print "============================>>>>> {}".format(xml)
                guid = str(uuid.uuid1())
                try:
                    row = rec_to_row(xml)
                    row = json_transform(row)
                    row["id"] = guid
                    row["ingest_id"] = args.ingest_id
                    row["case_id"] = args.case_id
                    row["alt_ref_id"] = args.alt_ref_id
                    row["label"] = args.label
                    row["original_artifact"] = {"filename" : os.path.basename(xl_file), "type" : "xl"}

                    spit(outfile, json.dumps(row)+ "\n")
                except Exception as e:
                    try:
                        _,name = os.path.split(xl_file)
                        _dir = "{}/{}_{}".format("tmp/failed", name, lex_date)
                        mkdirp(_dir)
                        spit("{}/{}.xml".format(_dir, guid), str(xml))
                    except:
                        print "Failed to log broken file!  Check dataset for Errors!"

                    traceback.print_exc()
                    count_failures += 1
                    print "FAILED to process xml message part.  Exception line: {} | {} ".format(j, e.message)

                if j % 100 == 0:
                    prn("completed line: {}".format(j))

        print "Completed processing xml file {}. Total messages={} Failures={}".format(xl_file, j, count_failures)
    print "Completed processing all xml files.  Check for failures above."