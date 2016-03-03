# Standard lib
import argparse
import json
import subprocess

# 3rd-party modules
from pyspark import SparkContext, SparkConf

def dump(x):
    return json.dumps(x)

# TODO
def language(text):
    return ""

def translate(text, from_lang, to_lang='en'):
    return text

def translate_hack(text, from_lang, to_lang='en'):
    if not text.strip():
        return ""

    # TODO this is slow
    cmd = ['apertium' , from_lang+'-'+to_lang]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    out_text, err = p.communicate(text)

    return out_text

def process_email(email):
    if "body" in email:
        lang = language(email["body"])
        if not lang == 'en':
            translated = translate_hack(email["body"], 'en','es')
            email["body_lang"]= lang
            email["body_translated"] = translated

    # if "subject" in email:
    #     lang = language(email["subject"])
    #     if not lang == 'en':
    #         translated = translate(email["subject"], lang)
    #         email["subject"]["lang"] = lang
    #         email["subject"]["translated"] = translated
    #
    # for attachment in email["attachments"]:
    #     if "contents" in attachment:
    #         lang = language(attachment["contents"])
    #         if not lang == 'en':
    #             translated = translate(attachment["contents"], lang)
    #             attachment["contents"]["lang"] = lang
    #             attachment["contents"]["translated"] = translated

    return email

def test():
    print process_email({'body':"hello world"})
    return

def process_patition(emails):
    for email in emails:
        yield process_email(email)

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
    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman translate")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_content_path).coalesce(50).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(lambda docs: process_patition(docs)).map(dump).saveAsTextFile(args.output_content_translated)
