#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, argparse
import re
import hashlib
import base64
import json
import email
import uuid
from email.utils import getaddresses, parsedate_tz

import quopri

import dateutil.parser 
import dateutil.tz
import datetime

sys.path.append("./utils")

from utils.functions import nth, head, counter
from utils.file import slurp, mkdirp, spit

def md5(sz):
    return hashlib.md5(sz).hexdigest()

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
    except TypeError:
        dt= datetime.datetime(*parsedate_tz(str_date)[:6])
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=dateutil.tz.tzutc())
    dt_tz = dt.astimezone(dateutil.tz.tzutc())
    return dt_tz.strftime('%Y-%m-%dT%H:%M:%S')


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

def createRow(email_id, mail, attach, msg_body, categories):
    #addr_tostr = lambda arr : ";".join(arr)
    #addrs = lambda arr : [clean_string(addr.lower(), [(r'\'', '')]) for
    #name, addr in getaddresses(arr)]

    #csv_sep = lambda arr : ",".join(arr) if arr else ''
    #scolon_sep = lambda arr : ";".join(arr) if arr else '' 

    ##
    ## return tuple (extracted emails array, unprocessed parts as array)
    ##
    def addrs(arr):
        items = []
        arr = [clean_string(s.lower(), [EXPR_OPTS['fix_utf8'], (r'\t', ';'), (r'\n', ';') ]) for s in arr]
        for name, addr in getaddresses(arr):
            if '@' in addr:
                items.append(addr)
            elif '@' in name:
                items.append(name)
        return  ([clean_string(s.lower(), [(r'\'', '')]) for s in items], arr)


    one = lambda arr : head(arr) if arr else ''

    originating_ips = mail.get_all('x-originating-ip', [])
    forensic_bcc = mail.get_all('x-libpst-forensic-bcc', [])
    msgid = [clean_string(s, [(r'\n', ''), EXPR_OPTS['fix_utf8'], EXPR_OPTS['fix_tab'] ]) for s in mail.get_all('message-id', [])] 
    inreplyto = [clean_string(s, [(r'\n', ''), EXPR_OPTS['fix_utf8'], EXPR_OPTS['fix_tab'] ]) for s in mail.get_all('in-reply-to', [])] 
    references = [clean_string(s, [(r'\n', ' '),  EXPR_OPTS['fix_utf8'], EXPR_OPTS['fix_tab'] ]) for s in mail.get_all('references', [])]
    mail_date= mail.get_all('date', None)
    subject = mail.get_all('subject', [])
    #importance ??
    #ip ??
    senders, senders_line = addrs(mail.get_all('from', []))
    #senders = [target_email if s == 'mailer-daemon' else s for s in senders]

    tos, tos_line = addrs(mail.get_all('to', []))
    ccs, ccs_line = addrs(mail.get_all('cc', []))
    bccs, bccs_line = addrs(mail.get_all('bcc', []))
    subject = clean_string(quopri.decodestring(one(subject)),
                     [            
                         EXPR_OPTS['fix_utf8'], 
                         EXPR_OPTS['fix_tab'], 
                         EXPR_OPTS['fix_cr']])

    body = clean_string(quopri.decodestring(msg_body),
                     [            
                         EXPR_OPTS['fix_utf8'], 
                         EXPR_OPTS['fix_tab'], 
                         EXPR_OPTS['fix_cr']])
    
    doc = { "id": email_id,
            "datetime": dateToUTCstr(head(mail_date)) if mail_date else 'NODATE',
            "originating_ips" : originating_ips,
            "categories" : categories,
            "senders": senders,
            "senders_line": senders_line,
            "tos": tos,
            "tos_line": tos_line,
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
            "body": body
    }
    return json.dumps(doc)

#return "\t".join([email_id, _dir, "", #scolon_sep(categories), dateToUTCstr(head(mail_date)) if mail_date else 'NODATE' , '', addr_tostr(senders), '', addr_tostr(tos), addr_tostr(ccs), addr_tostr(bccs), scolon_sep(attach), one(msgid), csv_sep(inreplyto), scolon_sep(references), subject, body])

# in: email as string
# out: map of meta information
# side_effect: write email to out_dir 
#     along with attachments
def extract(email_id, message, categories):
    #message = email.message_from_string(buff_mail)
    attach=[]
    msg = ""
    attach_count = counter()

    for part in message.walk():
        if part.get_content_type() == 'text/plain':
            msg = msg + "\n" + part.get_payload() 
        if part.get_content_type() == 'message/delivery-status':
            continue
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue

        fileName = part.get_filename()
        fileName = fileName.lower() if fileName else "attach_{}".format(attach_count.next())
        
        if fileName == 'rtf-body.rtf':
            continue

        fileName = clean_string(fileName, [
            EXPR_OPTS['fix_utf8'], 
            EXPR_OPTS['fix_forwardslash'], 
            (r' ', '_'),
            (r'&', '_')])
        
        _, extension = os.path.splitext(fileName.lower())
        filename_guid = str(uuid.uuid1())

        #filePath = "{}/attachments/{}{}".format(out_dir, filename_guid, extension)        
        # #save attachment
        # fp = open(filePath, 'wb')
        # fp.write(part.get_payload(decode=True))
        # fp.close()
        
        bstr = part.get_payload(decode=True)
        b64 = base64.b64encode(bstr)

        attach.append({ "filename" : fileName,
                        "guid" : filename_guid,
                        "extension" : extension,
                        "contents64" : b64
        })

    msg = clean_string(msg, [EXPR_OPTS['fix_utf8']])
    # writes raw message to txt file
    #spit("{}/{}.txt".format(_dir, email_id), msg)
    row= createRow(email_id, message, attach, msg, categories)

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


