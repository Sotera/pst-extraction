#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, argparse,  traceback
import re
import hashlib
import base64
import email
import uuid
from email.utils import getaddresses, parsedate_tz

import dateutil.parser
import dateutil.tz
import datetime

import chardet

from email.header import decode_header

sys.path.append("./utils")

from utils.functions import nth, head, counter
from utils.file import slurp, mkdirp, spit

def md5(sz):
    return hashlib.md5(sz).hexdigest()

def str_to_unicode(src_str, encoding='utf8'):
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
        return u''

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


def is_valid_utf8(text):
    try:
        text.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True

# sz raw string
# expr_list array of tuples (reg_exp, replacement)
def clean_string(sz, expr_list):
    return reduce(lambda x,r: re.sub(nth(r,0),nth(r,1,' '), x), expr_list, sz)


def dateToUTCstr(str_date):
    # this fails to parse timezones out of formats like
    # Tue, 17 Jun 2010 08:33:51 EDT
    # so it will assume the local timezone for those cases

    try:
        dt = dateutil.parser.parse(str_date)
    except (TypeError, ValueError) as e:
        # print u"Failed to parse date with dateutil, using email utils: date={}".format(str_date)
        parsed_dt = parsedate_tz(str_date)
        # Make an arbitrary tz info object name can be anything NSTZ "Newman Seconds Time Zone"
        nstz_info = dateutil.tz.tzoffset("NSTZ",parsed_dt[9])
        dt= datetime.datetime(*parsed_dt[:6], tzinfo=nstz_info)


    if not dt.tzinfo:
        print "WARNING:  Failed to parse timezone defaulting to UTC for Date: {}".format(str_date)
        dt = dt.replace(tzinfo=dateutil.tz.tzutc())

    dt_tz = dt.astimezone(dateutil.tz.tzutc())
    time_str =  dt_tz.strftime('%Y-%m-%dT%H:%M:%S')
    # print u"Parsed date={} ====> {}".format(str_date, time_str)

    return time_str



EXPR_OPTS = { 'fix_utf8' : (r'[^\x00-\x7F]', ' '),
              'fix_tab' : (r'\t', ' '),
              'fix_newline' : (r'\n', '[:newline:]'),
              'fix_cr' : (r'\r', ' '),
              'fix_forwardslash' : (r'/','_')
              }

# def headerrow():
#     row = "\t".join(['num','dir','category','datetime','importance','from','ip','to','cc','bcc','attachments','messageid','inreplyto','references','subject','body'])
#     return row

def categoryList(orginal_path):
    path = os.path.normpath(orginal_path)
    return filter(lambda x: x, path.split(os.sep))


##
## add TARGET email as a BCC if not present in any field
##
def bccList(target, senders, tos, ccs, bccs):
    if target.lower() in [s.lower() if s else "" for s in set(senders + tos + ccs + bccs)]:
        return bccs
    return bccs + [target]



# text will be in the legacy encoding with internationalization headers
# e.g.
# =?iso-8859-1?q?p=F6stal?=
# see https://docs.python.org/3/library/email.header.html
def convert_encoded(text):
    try:
        decoded_header = decode_header(text)
        return u''.join([ str_to_unicode(str, charset or 'utf-8') for str, charset in decoded_header ])

        # Original version
        # return u''.join([ unicode(str, charset or 'utf-8') for str, charset in decoded_header ])
    except:
        return text

    ##
## return tuple (extracted emails array, unprocessed parts as array)
##
def addrs(raw_addrs):
    items = []

    # TODO remove the cleaner!
    # raw_addrs = [clean_string(convert_encoded(s.lower()),[EXPR_OPTS['fix_utf8'],(r'\t', ';'), (r'\n', ';') ]) for s in raw_addrs]
    # arr = [clean_string(s.lower(), [EXPR_OPTS['fix_utf8'], (r'\t', ';'), (r'\n', ';') ]) for s in arr]
    utf8_addrs = [convert_encoded(raw_addr) for raw_addr in raw_addrs]

    for name, addr in getaddresses(utf8_addrs):
        if u'@' in addr:
            items.append(addr)
        elif u'@' in name:
            items.append(name)
    return  ([clean_string(s.lower(), [(r'\'', u'')]) for s in items], utf8_addrs)


def createRow(email_id, mail, attach, msg_body, body_type, categories):

    one = lambda arr : head(arr) if arr else ''

    originating_ips = mail.get_all('x-originating-ip', [])
    forensic_bcc = mail.get_all('x-libpst-forensic-bcc', [])
    msgid = [clean_string(s, [(r'\n', ''), EXPR_OPTS['fix_utf8'], EXPR_OPTS['fix_tab'] ]) for s in mail.get_all('message-id', [])]
    inreplyto = [clean_string(s, [(r'\n', ''), EXPR_OPTS['fix_utf8'], EXPR_OPTS['fix_tab'] ]) for s in mail.get_all('in-reply-to', [])]
    references = [clean_string(s, [(r'\n', ' '),  EXPR_OPTS['fix_utf8'], EXPR_OPTS['fix_tab'] ]) for s in mail.get_all('references', [])]
    mail_date= mail.get_all('date', None)

    subject = mail.get_all('subject', [])
    subject = convert_encoded(one(subject))

    senders, senders_line = addrs(mail.get_all('from', []))

    # Parse and convert all addresses to utf-8
    # results are tuple (addr, addr_line) where the _line variables contain the full email header and
    # the first value is a list of only the address portion

    tos, tos_line = addrs(mail.get_all('to', []))
    delivered_to, delivered_to_line = addrs(mail.get_all('Delivered-To', []))
    # Sometime tos is the string "Undisclosed-Recipient" in which case we can try the delivery address
    if not tos and delivered_to:
        tos = delivered_to
    ccs, ccs_line = addrs(mail.get_all('cc', []))
    bccs, bccs_line = addrs(mail.get_all('bcc', []))

    doc = { "id": email_id,
            "datetime": dateToUTCstr(head(mail_date)) if mail_date else None,
            "originating_ips" : originating_ips,
            "categories" : categories,
            "senders": senders,
            "senders_line": senders_line,
            "tos": tos,
            "tos_line": tos_line,
            "delivered_to": delivered_to,
            "delivered_to_line": delivered_to_line,
            "ccs": ccs,
            "ccs_line" : ccs_line,
            "bccs": bccs,
            "bccs_line" : bccs_line,
            "forensic-bcc" : forensic_bcc,
            "attachments": attach,
            "messageid": msgid,
            "inreplyto": inreplyto,
            "references": references,
            "subject": subject,
            "body": msg_body,
            "body_as_html": body_type == 'html'
            }
    return doc

def decode_body(part):
    decode = part.get_all('Content-Transfer-Encoding', [''])[0].lower() == 'base64' or part.get_all('Content-Transfer-Encoding', [''])[0].lower() == 'quoted-printable'
    charset = part.get_content_charset()

    text = part.get_payload(decode=decode)
    text = str_to_unicode(text, encoding=charset)

    return text

# in: email as string
# out: map of meta information
# side_effect: write email to out_dir 
#     along with attachments
def extract(email_id, message, categories, preserve_attachments=True):
    #message = email.message_from_string(buff_mail)
    attach=[]
    msg = u''
    body_type = "text"
    attach_count = counter()

    for part in message.walk():
        # We only want plain text which is not an attachment
        # TODO do we need to handle fileName == 'rtf-body.rtf'?
        valid_utf8 = True

        # Email body may be provided in text, text + html seperate parts or rarely as html only,
        if part.get_content_type() == 'text/plain' and part.get_filename(None) is None:

            body_type = "text"
            if msg:
                msg += u"\n=============================Next Part==============================\n"
            decode = part.get_all('Content-Transfer-Encoding', [''])[0] == 'base64' or part.get_all('Content-Transfer-Encoding', [''])[0] == 'quoted-printable'

            charset = part.get_content_charset()

            text = part.get_payload(decode=decode)
            if not charset:
                chardet.detect(text)

            text = str_to_unicode(text, encoding=charset)
            msg += text

            # writes raw message to txt file
            # spit("{}/{}.txt".format("tmp", email_id), text)
        # Handle html only body
        elif part.get_content_type() == 'text/html' and part.get_filename(None) is None:
            html = decode_body(part)
            body_type = "html"
            if msg:
                # If there is already text in the msg then just treat the html as raw text
                msg += u"\n=============================Next Part==============================\n"
                body_type = "text"

            msg += html

        if part.get_content_type() == 'message/delivery-status':
            continue
        if part.get_content_maintype() == 'multipart':
            continue

        if part.get('Content-Disposition') is None:
            continue

        fileName = part.get_filename()

        if not fileName and not preserve_attachments:
            continue

        fileName = convert_encoded(fileName) if fileName else "attach_{}".format(attach_count.next())

        if fileName == 'rtf-body.rtf':
            continue

        _, extension = os.path.splitext(fileName.lower())
        filename_guid = str(uuid.uuid1())

        content_type = part.get_content_type()

        #filePath = "{}/attachments/{}{}".format(out_dir, filename_guid, extension)        
        # #save attachment
        # fp = open(filePath, 'wb')
        # fp.write(part.get_payload(decode=True))
        # fp.close()

        bstr = part.get_payload(decode=True)
        b64=''
        if bstr:
            b64 = base64.b64encode(bstr)

        attach.append({ "filename" : fileName,
                        "guid" : filename_guid,
                        "extension" : extension,
                        "filesize": len(bstr) if bstr else 0,
                        "contents64" : b64,
                        "content_type" : content_type
                        })

    # writes raw message to txt file
    #spit("{}/{}.txt".format(_dir, email_id), msg)

    try:
        row = createRow(email_id, message, attach, msg, body_type, categories)
    except Exception as e:
        print "Failed to process message: {} Exception:".format(email_id, e)
        print traceback.format_exc()
        raise e

    return row


if __name__ == "__main__":

    desc = '''
examples:
    ./this.py email.eml
    '''
    parser = argparse.ArgumentParser(
        description=" ... ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    #parser.add_argument("target_email", help="target email")
    #parser.add_argument("outdir", help="Out Dir")
    parser.add_argument("file_path", help="File Path")
    #parser.add_argument("infile", nargs='?', type=argparse.FileType('r'), default=sys.stdin, help="Input File")
    args = parser.parse_args()
    guid = md5(args.file_path)
    category = categoryList(args.file_path)
    buff_msg = slurp(args.file_path)
    message = email.message_from_string(buff_msg)
    row = extract(guid, message, category)

    print row
