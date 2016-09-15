# -*- coding: utf-8 -*-

import sys, os
import json
import uuid



def mkdir(path):
    os.makedirs(path)

def mkdirp(path):
    if not os.path.exists(path):
        mkdir(path)
def spit(filePath, data, overwrite=False):
    mode= 'w' if overwrite else 'a'
    with open(filePath, mode) as x: x.write(data)

def valid_json_filter(script_name, lex_date, skip_filter, doc):
    if skip_filter:
        return True
    try:
        json.loads(doc)
        return True
    except :
        e = sys.exc_info()[0]
        try:
            error_msg = "{} - FAILED to map json -- {}\n\n".format(script_name, e)
            print error_msg
            print u"{} - Json row prefix [:10000] -- {}".format(script_name, doc[:10000])

            _dir = "{}/{}_{}".format("tmp/failed", script_name, lex_date)
            mkdirp(_dir)
            guid = str(uuid.uuid1())


            spit(u"{}/failed-{}.log".format(_dir, guid), error_msg )
            spit(u"{}/failed-{}.log".format(_dir, guid), doc)
        except:
            print "Failed to log broken file!  Check dataset for Errors!"
        return False