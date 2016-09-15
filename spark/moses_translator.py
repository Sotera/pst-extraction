#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys
import time
import xmlrpclib
import argparse

import re
import sys
import unicodedata

abbrevs = [u"mr.", u"mrs.", u"ms.", u"mme.", u"dr.", u"a.m.", u"p.m.", u"u.s.", u"e.u.", u"i.d.", u"d.c.", u"t.b.", u"p.s.", u"m.s.", u"m.a.", u"u.n",
           u"a.", u"b.", u"c.", u"d.", u"e.", u"f.", u"g.", u"h.", u"i.", u"j.", u"k.", u"l.", u"m.", u"n.", u"o.", u"p.", u"q.", u"r.", u"s.", u"t.",
           u"u.", u"v.", u"w.", u"x.", u"y.", u"z.", u"etc.", u"e.t.", u"a.d.", u"b.c.", u"e.g.", u"i.e.",  u"'em", u"...", u"....", u"--", u"'bout", u"'s"]

class MosesServer:

    def __init__(self, args=[]):
        self.url = None
        self.proxy = None

    def connect(self, url):
        if url[:4] != "http":
            url = "http://%s" % url
        if url[-5:] != "/RPC2":
            url += "/RPC2"
        self.url = url
        self.proxy = xmlrpclib.ServerProxy(self.url)

    def tokenize_en(self, input_str):
        words = input_str.split()
        num_words = len(words)
        i = 0
        while i < num_words:
            w = words[i]
            if not w in abbrevs and len(w) > 1:
                if 'P' in unicodedata.category(w[-1]):
                    words[i:i+1] = [w[:-1], w[-1:]]
                    num_words += 1
                    i -= 1
                elif 'P' in unicodedata.category(w[0]):
                    words[i:i+1] = [w[0:1], w[1:]]
                    num_words += 1
            # end rec split
            i += 1

        return words


    def translate(self, input):
        attempts = 0
        while attempts < 100:
            try:
                if type(input) is unicode:
                    # If the server does not expect unicode, provide a
                    # properly encoded string!
                    param = {'text': input.strip().encode('utf8')}
                    return self.proxy.translate(param)['text'].decode('utf8')

                elif type(input) is str:
                    param = {'text': input.strip()}
                    return self.proxy.translate(param)['text']

                elif type(input) is list:
                    return [self.translate(x) for x in input]

                elif type(input) is dict:
                    return self.proxy.translate(input)

                else:
                    raise Exception("Can't handle input of this type!")

            except:
                attempts += 1
                print >> sys.stderr, "WAITING", attempts
                time.sleep(1)
        raise Exception("Translation request failed")


if __name__ == '__main__':
    desc='regexp extraction'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    # parser.add_argument("input_content_path", help="input email or attachment content path")
    # parser.add_argument("output_content_translated", help="output all text enriched with translation and language")
    # args = parser.parse_args()


    moses = MosesServer()
    moses.connect("http://10.1.70.126:8080/RPC2")
    en_text = moses.translate("gracias por siempre fernando valenzuela")
    print en_text


    es_query=u' '.join(moses.tokenize_en(u"gracias por siempre fernando valenzuela")).encode("utf-8")
    en_text = moses.translate(es_query)
    print en_text
    # TEST
    # test()
    # print "done"

    # SPARK

    print "done."