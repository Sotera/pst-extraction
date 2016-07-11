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
import ExtractMsg
import base64
import re
import chardet

# from imapclient import imap_utf7
from email_extract_json_unicode import addrs
from utils.functions import nth, head, counter
from email.utils import getaddresses, parsedate_tz

sys.path.append("./utils")

from utils.file import slurpBase64, RollingFile

def timeNow():
    return datetime.datetime.now().strftime('%H:%M:%S')

# def xstr(s, default=u''):
#     return default if s is None else str(s)

def str_to_unicode(src_str, encoding='utf8', default=u''):
    """
    Converts Python byte strings to Unicode (using the chardet module to try and detect the byte
    encoding if necessary).

    Args:
        src_str <str> - a Python byte string

        encoding <str> - (optional) name of charset/encoding for the byte string (i.e., "to know
            what glyphs the bytes in src_str refer to, use this encoding scheme). Examples:
            'ascii', 'big5' (chinese), etc. (for a complete list see https://docs.python.org/2/library/codecs.html).
            Note that this defaults to 'utf8' because ultimately it will be passed to the unicode()
            function, which uses utf8 by default.
    """
    if src_str is None:
        return default

    elif isinstance(src_str, unicode):
        return src_str

    elif src_str == '':
        # Because empty string can safely/easily be converted to unicode, whereas if we end up trying
        # to detect the encoding below we can (unnecessarily) encounter errors since you can't detect
        # the encoding of an empty string.
        return unicode(src_str)

    if not encoding:
        encoding = 'utf8'

    try:
        # Attempt conversion to unicode (assumes 'utf8' encoding by default)
        unicode_str = unicode(src_str, encoding, 'replace')
        return unicode_str

    except (UnicodeDecodeError, LookupError, ValueError) as err:
        print("Failed to convert byte string to Unicode using '%s' codec; will attempt to detect encoding" % encoding)

    # If we get this far it means the string is in some encoding other than what was specified.
    # Instead of just giving up we'll try to detect the encoding by actually looking at the bytes.
    charset_guess_dict = chardet.detect(src_str)
    if charset_guess_dict is not None and charset_guess_dict.get('encoding'): # Ensure detected encoding is not None or empty string
        detected_encoding = charset_guess_dict['encoding']
        try:
            # Attempt conversion to unicode (assumes 'utf8' encoding by default)
            unicode_str = unicode(src_str, detected_encoding, 'replace')
            print("Recovered from previous unicode conversion failure; detected encoding '%s' and successfully converted to Unicode" % detected_encoding)
            return unicode_str

        except (UnicodeDecodeError, LookupError, ValueError) as err:
            print("Failed to convert byte string to Unicode using detected '%s' codec; using ascii version of byte hex values" % detected_encoding)

    # If we get this far it means all of our attempts to propertly decode the byte string have failed.
    # Our last resort is to convert the bytes to an ASCII hex string (e.g., \xfd, etc.)
    ascii_repr_str = repr(src_str)
    return unicode(ascii_repr_str, 'ascii', 'replace')


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
count_success = 0
count_failures = 0

def guess_mime(filename):
    mime_type = mimetypes.guess_type(filename)[0]
    return mime_type if mime_type else "application/octet-stream"

def UTC_date(date_str):
    dt = dateutil.parser.parse(date_str)
    if not dt.tzinfo:
        dt =dt.replace(tzinfo=dateutil.tz.tzlocal())
        dt = dt.astimezone(dateutil.tz.tzutc())

        return dt.strftime('%Y-%m-%dT%H:%M:%S')

def dateToUTCstr(str_date):
    str_date = '' if not str_date else str_date
    try:
        dt = dateutil.parser.parse(str_date)
    except TypeError:
        dt= datetime.datetime(*parsedate_tz(str_date)[:6])
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=dateutil.tz.tzutc())
    dt_tz = dt.astimezone(dateutil.tz.tzutc())
    return dt_tz.strftime('%Y-%m-%dT%H:%M:%S')

def get_attachment_name(msg_attachment):
    filename = msg_attachment.longFilename
    if not filename:
        filename = msg_attachment.shortFilename
    if not filename:
        filename = str(uuid.uuid1())
    return filename

def get_attachments_json(msg):
    if not msg.attachments:
        return []

    attach_count = counter()
    attach=[]

    for attachment in msg.attachments:
        b64 = base64.b64encode(attachment.data)
        fileName = attachment.longFilename
        if not fileName:
            fileName = attachment.shortFilename
        if not fileName:
            fileName = "attach_{}".format(attach_count.next())

        _, extension = os.path.splitext(fileName.lower())

        #TODO replace this call to "guess"  with the mime type stored in the msg attachment
        mime_type = mimetypes.guess_type(fileName)[0]

        attach.append({ "filename" : fileName,
                        "guid" : str(uuid.uuid1()),
                        "extension" : extension,
                        "filesize": len(attachment.data) if attachment.data else 0,
                        "contents64" : b64,
                        "content_type" : mime_type
                        })
    return attach

def parse_msg(filename):
    msg = ExtractMsg.Message(filename)

    attachments = get_attachments_json(msg)

    guid = str(uuid.uuid1())
    senders, senders_line = addrs(re.split('[,;]',str_to_unicode(msg.sender, default=u"NoSender")))
    tos, tos_line = addrs(re.split('[,;]',str_to_unicode(msg.to, default=u"NoRcvr")))
    ccs, ccs_line = addrs(re.split('[,;]',str_to_unicode(msg.cc)))
    try:
        date = dateToUTCstr(msg.date)
    except:
        print "FAILED to parse msg date.  Setting date to default value for filename {}".format(str(filename))
        date = "2010-01-01T00:00:00"

    return {
        'id': guid,
        'senders_line': senders_line,
        'senders': senders,
        'tos_line': tos_line,
        'tos': tos,
        'ccs_line': ccs_line,
        'ccs': ccs,
        'bccs_line': '',
        'bccs': [],
        'subject': str_to_unicode(msg.subject),
        'datetime': date,
        'attachments': attachments,
        'body': str_to_unicode(msg.body)
        # TODO make sure this is noo needed
        # 'body': imap_utf7.decode(msg.body)
    }

MSG_FILE_TYPES=['msg']

def crawl_files(root_dir, meta):
    global count_total
    global count_failures
    global count_success

    _prefix_length = len(root_dir)
    for root, _, files in os.walk(root_dir):
        for filename in files:
            try:
                _, ext = os.path.splitext(filename)
                if ext.replace(".","").lower() not in MSG_FILE_TYPES:
                    print "Skipping message - not a 'msg' type: %s"%str(filename)
                    continue

                count_total+=1
                print "Processing message: %s"%str(filename)
                abs_path = os.path.abspath("{}/{}".format(root, filename))
                rel_path = str(abs_path[(_prefix_length if not abs_path[_prefix_length]=='/' else _prefix_length+1):])
                print "-- abs_path: %s"%str(abs_path)
                print "-- rel_path: %s"%str(rel_path)

                meta["original_artifact"] = {"filename" : rel_path, "type" : "msg"}
                row = parse_msg(abs_path)
                row.update(meta)
                count_success+=1
                yield json.dumps(row)
            except Exception as e:
                traceback.print_exc()
                count_failures += 1
                print "FAILED to process msg message file.  Exception Filename: {} | {} | {} ".format(filename, abs_path, e.message)

if __name__ == "__main__":

    desc = '''
examples:
    ./msg.py {files_directory} output_path
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
    print "Total failed: {}".format(count_failures)
    print "Total success: {}".format(count_success)
