#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import imaplib
import email
from email.utils import getaddresses
import datetime
import getpass
import argparse

# functions
def timeNow():
    return datetime.datetime.now().strftime('%H:%M:%S')
 
def spit(filePath, data, overwrite=False):
    # write all contents to a file
    mode= 'w' if overwrite else 'a'
    with open(filePath, mode) as x: x.write(data)

def mkdir(path):
    os.makedirs(path)

def inc(n):
    return n+1

def counter(start=0):
    n = start
    while True:
        yield n
        n = inc(n)

class login(object):

    def __init__(self, username, passwd):
        self.usr = username
        self.pwd = passwd
        self._session = None

    def session(self):
        return self._session

    def __enter__(self):
        self._session = imaplib.IMAP4_SSL('imap.gmail.com')
        resp, account = self._session.login(self.usr, self.pwd)        
        if resp != 'OK':
            raise Exception("Failed login: %s %s".format(resp, account))
        return self

    def __exit__(self, type, value, traceback):
        self._session.close()
        self._session.logout()

UID_RE = re.compile(r"\d+\s+\(UID (\d+)\)$")

def getUIDForMessage(srv, n):
    resp, lst = srv.fetch(n, 'UID')
    m = UID_RE.match(lst[0])
    if not m:
        raise Exception(
            "Internal error parsing UID response: %s %s.  Please try again" % (resp, lst))
    return m.group(1)

def download(srv, outdir, limit):
    srv.select("[Gmail]/All Mail", True)
    #resp, data = srv.uid('SEARCH', None, 'ALL')
    resp, data = srv.search(None, 'ALL')

    if resp != 'OK':
        raise Exception("Error searching: %s %s" % (resp, data))

    msgids = data[0].split()

    if limit > 0:
        msgids = msgids[-limit:]

    c = counter()
    l = len(msgids)
    
    for msgid in msgids:
        uid = getUIDForMessage(srv, msgid)
        fldr ="{}/".format(outdir)
        i = c.next()
        
        if i % 200 == 0:
            print "[{}] Downloaded: {}/{}".format(timeNow(), i,l)

        resp, msgParts = srv.fetch(msgid, '(RFC822)')
        if resp != 'OK':
            raise Exception("Bad response: %s %s" % (resp, msgParts))

        emailBody = msgParts[0][1]
        spit("{}/{}.eml".format(fldr, uid), emailBody)

if __name__ == "__main__":

    desc='gmail downloader '

    parser = argparse.ArgumentParser(
        description="GMail Downloader", 
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("-o", "--output_dir", default="data/", help="directory to save emails too")
    parser.add_argument("-l", "--limit", type=int, default=2000, help="limit the number of messages downloaded, default 2000, -1 for all messages")

    args = parser.parse_args()
    username = raw_input('Enter your GMail username:')
    passwd = getpass.getpass('Enter your password: ')

    with login(username, passwd) as conn:    
        fldr = "{}/{}".format(args.output_dir, username)

        if not os.path.exists(fldr):
            mkdir(fldr)
            
        download(conn.session(), fldr, args.limit)


