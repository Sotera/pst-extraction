# Standard lib
import sys
import os
import argparse
import json
import subprocess

from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
import datetime
from functools import partial
from filters import valid_json_filter

# 3rd-party modules
from pyspark import SparkContext, SparkConf

SUPPORTED_LANGS = ['es','en']

def dump(x):
    return json.dumps(x)

def language(text, override_language=None):
    if override_language:
        return override_language

    try:
        return detect(text)
    except LangDetectException:
        return 'en'

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

def translate(text, moses, from_lang, to_lang='en'):
    if moses:
        return translate_moses(text, moses, from_lang, to_lang='en')

    return translate_apertium(text, from_lang, to_lang='en')

def process_email(email, force_language, translation_mode, moses):
    # default to en

    lang = 'en'

    if "body" in email:
        lang = language(email["body"], force_language)
        if not lang == 'en':
            translated = translate(email["body"], moses, lang, 'en')
            email["body_lang"]= lang
            email["body_translated"] = translated

    if "subject" in email:
        # TODO  -- fix this for now use body lang for subject because library seems to miscalculate it because of RE: FW: characters etc
        # lang = language(email["subject"], force_language)
        if not lang == 'en':
            translated = translate(email["subject"], moses, lang, 'en')
            email["subject_lang"] = lang
            email["subject_translated"] = translated

    for attachment in email["attachments"]:
        if "content" in attachment:
            lang = language(attachment["content"], force_language)
            if not lang == 'en':
                translated = translate(attachment["content"], moses, lang, 'en')
                attachment["content_lang"] = lang
                attachment["content_translated"] = translated

    return email


def process_patition(emails, force_language, translation_mode, moses_server):
    moses = None
    if translation_mode == 'moses' and moses_server:
        sys.path.append(".")
        from moses_translator import MosesServer
        moses = MosesServer()
        moses.connect(moses_server)

    for email in emails:
        yield process_email(email, force_language, translation_mode, moses)


def test():
    print process_email({'body':"hello world"})
    return

if __name__ == '__main__':
    desc='regexp extraction'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)


    # TEST
    # test()
    # print "done"

    # SPARK

    parser.add_argument("input_content_path", help="input email or attachment content path")
    parser.add_argument("output_content_translated", help="output all text enriched with translation and language")
    parser.add_argument("--force_language", default='en', help="Force the code to assume translation from a specific language to english.  If this is set to 'en' no translation will occur.")
    parser.add_argument("--translation_mode", help="Can be set to 'moses'|'apertium', moses requires a moses_server property set.")
    parser.add_argument("--moses_server", default="localhost:8080", help="Moses server to connect to for translation.")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()

    print "Translation params:  --force_language %s --translation_mode %s --moses_server %s"%( args.force_language, args.translation_mode, args.moses_server)

    conf = SparkConf().setAppName("Newman translate")
    sc = SparkContext(conf=conf)

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)

    rdd_emails = sc.textFile(args.input_content_path).filter(filter_fn).coalesce(50).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(lambda docs: process_patition(docs, args.force_language, args.translation_mode, args.moses_server)).map(dump).saveAsTextFile(args.output_content_translated)
