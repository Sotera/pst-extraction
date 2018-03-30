# Standard lib
# -*- coding: utf-8 -*-
import sys
import os
import argparse
import json
import urllib2
import subprocess

from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
import datetime
from functools import partial
from filters import valid_json_filter

# 3rd-party modules
from pyspark import SparkContext, SparkConf
SUPPORTED_LANGS = ['es','en','fa','ar','spanish','persian','arabic']
JOSHUA_ENDPOINT = '/joshua/translate/'
LANGUAGE_ALIASES = {
    'ar': 'arabic',
    'cz': 'czech',
    'da': 'danish',
    'nl': 'dutch',
    'en': 'english',
    'et': 'estonian',
    'fa': 'persian',
    'fi': 'finnish',
    'fr': 'french',
    'de': 'german',
    'el': 'greek',
    'it': 'italian',
    'no': 'norwegian',
    'pl': 'polish',
    'pt': 'portuguese',
    'sl': 'slovene',
    'es': 'spanish',
    'sv': 'swedish',
    'tr': 'turkish'
}

def dump(x):
    return json.dumps(x)


def get_lang(string):
    print 'get lang 1'
    parts = string.split('\n')
    langs = {}
    for part in parts:
        if len(part) == 0:
            continue
        try:
            print 'detecting: ' + part.encode('utf-8')
            lang = detect(part)
        except LangDetectException as e:
            print e
            continue
        print 'detected: ' + lang
        if lang == 'en':
            continue
        langs[lang] = langs.get(lang, 0) + 1
    lang = 'en'
    count = 0
    for k,v in langs.iteritems():
        if v > count and k in SUPPORTED_LANGS:
            lang = k
            count = v
    print 'RETURNING : ' + lang
    return lang


def language(text, override_language=None):

    if override_language and override_language != 'auto':
        return override_language

    print 'detecting language'
    lang = get_lang(text)
    print "Detected :" + lang
    return lang


def translate_apertium_pipe(text, from_lang, to_lang='en'):
    return text

def translate_apertium(text, from_lang, to_lang='en'):
    if not text.strip():
        return ""
    if not to_lang in SUPPORTED_LANGS:
        return text

    # TODO this is slow
    cmd = ['apertium' , from_lang+'-'+to_lang]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    out_text, err = p.communicate(text.encode("utf-8"))

    return out_text if not out_text.startswith("Error: Mode") else text

def translate_moses(text, moses, from_lang, to_lang='en'):
    es_query=u' '.join(moses.tokenize_en(text)).encode("utf-8")
    return moses.translate(es_query)

def translate_joshua(text, joshua_server, from_lang, to_lang='en'):
    print 'translating text'
    data = {'inputLanguage': LANGUAGE_ALIASES[from_lang], 'inputText': text}
    data = json.dumps(data)

    url = 'http://' + joshua_server + JOSHUA_ENDPOINT + LANGUAGE_ALIASES[to_lang]
    print url
    print data
    req = urllib2.Request(url, data, {'Content-Type': 'application/json'})

    f = urllib2.urlopen(req)
    translation = json.load(f)['outputText']
    f.close()
    print 'text translated ' + translation
    return translation

def translate(text, translation_mode, moses, joshua_server, from_lang, to_lang='en'):
    if translation_mode == 'moses' and moses:
        return translate_moses(text, moses, from_lang, to_lang='en')
    elif translation_mode == 'joshua' and joshua_server:
        print 'Translating text with joshua'
        return translate_joshua(text, joshua_server, from_lang, to_lang='en')

    return translate_apertium(text, from_lang, to_lang='en')

def process_email(email, force_language, translation_mode, moses, joshua_server):
    # default to en
    lang = 'en'

    if "body" in email:
        lang = language(email["body"], force_language)
        if not lang == 'en' and lang in SUPPORTED_LANGS:
            translated = translate(email["body"], translation_mode, moses, joshua_server, lang, 'en')
            email["body_lang"] = lang
            email["body_translated"] = translated

    if "subject" in email:
        # TODO  -- fix this for now use body lang for subject because library seems to miscalculate it because of RE: FW: characters etc
        # lang = language(email["subject"], force_language)
        if not lang == 'en' and lang in SUPPORTED_LANGS:
            translated = translate(email["subject"], translation_mode, moses, joshua_server, lang, 'en')
            email["subject_lang"] = lang
            email["subject_translated"] = translated

    #if "attachments" in email:
    #    for attachment in email["attachments"]:
    #        if "content" in attachment:
    #            lang = language(attachment["content"], force_language)
    #            if not lang == 'en' and lang in SUPPORTED_LANGS:
    #                translated = translate(attachment["content"], translation_mode, moses, joshua_server, lang, 'en')
    #                attachment["content_lang"] = lang
    #                attachment["content_translated"] = translated

    return email


def process_partition(emails, force_language, translation_mode, moses_server, joshua_server):
    moses = None
    if translation_mode == 'moses' and moses_server:
        sys.path.append(".")
        from moses_translator import MosesServer
        moses = MosesServer()
        moses.connect(moses_server)

    for email in emails:
        yield process_email(email, force_language, translation_mode, moses, joshua_server)


def test():
    print process_email({'body':u'Hello bob, I really want to go to the beach this weekend. \n How about you and I make a trip out of it, we can stay near the beach in a hotel if you like. \n La oración a modo de ejemplo debe ayudar al usuario a entender no solamente el significado de la palabra.'}, 'auto', 'joshua', 'localhost:8080', '10.1.70.200:8001')
    return

if __name__ == '__main__':
    desc='regexp extraction'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)


    # TEST
    #test()
    #print "done"
    #sys.exit()
    # SPARK

    parser.add_argument("input_content_path", help="input email or attachment content path")
    parser.add_argument("output_content_translated", help="output all text enriched with translation and language")
    parser.add_argument("--force_language", default='en', help="Force the code to assume translation from a specific language to english.  If this is set to 'en' no translation will occur.")
    parser.add_argument("--translation_mode", help="Can be set to 'moses'|'apertium'|'joshua', moses/joshua requires a moses_server/joshua_server property set.")
    parser.add_argument("--moses_server", default="localhost:8080", help="Moses server to connect to for translation.")
    parser.add_argument("--joshua_server", default="localhost:8001", help="Joshua server to connect to for translation.")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()

    print "Translation params:  --force_language %s --translation_mode %s --moses_server %s --joshua_server %s"%( 'auto', args.translation_mode, args.moses_server, args.joshua_server)

    conf = SparkConf().setAppName("Newman translate")
    sc = SparkContext(conf=conf)

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)

    rdd_emails = sc.textFile(args.input_content_path).filter(filter_fn).coalesce(50).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(lambda docs: process_partition(docs, 'auto', args.translation_mode, args.moses_server, args.joshua_server)).map(dump).saveAsTextFile(args.output_content_translated)
