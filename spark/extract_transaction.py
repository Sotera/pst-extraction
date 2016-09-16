#!/usr/bin/env python
# -*- coding: utf-8 -*-

import locale
import re
import sys
import argparse
import json
import csv
import uuid
import os
import datetime
from filters import valid_json_filter
from functools import partial

from pyspark import SparkContext, SparkConf



def dump(x):
    return json.dumps(x)

def make_csv(emails):
    filename = "tmp/"+str(uuid.uuid4())+".csv"
    with open(filename, 'wb') as csv_file:
        csv_writer=csv.writer(csv_file)

        for email in emails:
            rows=[]
            for currency in email["currency_entities"]:
                rows.append([email["senders"][0], str(email["tos"]), currency])

            # print str(rows)
            csv_writer.writerows (rows)



# Currently working regex (without spaces between $ and digits):
_REGEX = ur'((?:0|[0-9]\d{0,3}(?:,?\d{3})*)(?:\.\d+)?)'

# _REGEX_ORIG = ur'([$])((?:0|[1-9]\d{0,3}(?:,?\d{3})*)(?:\.\d+)?)'

# _REGEX=r'^\$?(?=\(.*\)|[^()]*$)\(?\d{1,3}(,?\d{3})?(\.\d\d?)?\)?$'
# _REGEX=r'^\$?(?=\(.*\)|[^()]*$)\(?\d{1,3}(,?\d{3})?(\.\d\d?)?\)?$'
# _REGEX=ur'^[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{2})?|(?:,[0-9]{3})*(?:\.[0-9]{2})?|(?:\.[0-9]{3})*(?:,[0-9]{2})?)$'
# _REGEX=r'(^| )[$]?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{2})?|(?:,[0-9]{3})*(?:\.[0-9]{2})?|(?:\.[0-9]{3})*(?:,[0-9]{2})?)'

CURRENCY_CHAR_WINDOW = 6
TRANSACTION_CHAR_WINDOW = 25
# TODO do something better
transaction_keyword_list = ['balance', 'paid','deposit', 'withdraw', 'bill', 'receipt', 'total']
# TODO LOAD from localization api etc
currency_key_list = ["US$"]

def gen_currency_keys():
    locales=('en_AU.utf8', 'en_BW.utf8', 'en_CA.utf8', 'en_DK.utf8', 'en_GB.utf8', 'en_HK.utf8', 'en_IE.utf8', 'en_IN', 'en_NG', 'en_PH.utf8', 'en_US.utf8', 'en_ZA.utf8', 'en_ZW.utf8', 'ja_JP.utf8')
    currency_keys = []
    for l in locales:
        try:
            locale.setlocale(locale.LC_ALL, l)
            conv=locale.localeconv()
            print('{ics} ==> {s}'.format(ics=conv['int_curr_symbol'], s=conv['currency_symbol']))
            currency_key_list.append(conv['int_curr_symbol'].upper())
            currency_key_list.append(conv['currency_symbol'])
        except Exception as e:
            print "FAILED to load locale for {}. {}".format(l, e)


def contains_transaction_key(match, text):
    excerpt_start = max(match.start() - TRANSACTION_CHAR_WINDOW, 0)

    excerpt_prefix = text[ excerpt_start : match.start() ]

    excerpt_stop = min(match.end() + TRANSACTION_CHAR_WINDOW, len(text)-1)
    excerpt_suffix = text[ match.start():excerpt_stop ]

    excerpt = text[excerpt_start : excerpt_stop]
    excerpt_value_start = match.start() - excerpt_start
    excerpt_value_stop = excerpt_stop - match.end()

    # If the sample string contains *any* of the keywords return true
    if any(transaction_keyword in excerpt.lower() for transaction_keyword in transaction_keyword_list):
        return (True, excerpt, match.start(), match.end())

    return (False, excerpt)


# TODO make return tuple (SYMBOL, DISTANCE) or None
# TODO distance from the currency symbol to number NOT TRUE|FALSE
def contains_currency_symbol(match, text):
    excerpt_start = max(match.start() - CURRENCY_CHAR_WINDOW, 0)

    excerpt_prefix = text[ excerpt_start : match.start() ]

    excerpt_stop = min(match.end() + CURRENCY_CHAR_WINDOW, len(text)-1)
    excerpt_suffix = text[ match.start():excerpt_stop ]

    excerpt = text[excerpt_start : excerpt_stop]
    excerpt_value_start = match.start() - excerpt_start
    excerpt_value_stop = excerpt_stop - match.end()


    # If the sample string contains *any* of the keywords return true
    if any(currency_key in excerpt.upper() for currency_key in currency_key_list):
        return (True, excerpt, match.start(), match.end())

    return (False, excerpt)

def currency(full_text_str):
    tagged_currency_entities = []
    # symbol = '$'
    text = full_text_str
    total = 0
    for match in re.finditer(_REGEX, text, re.MULTILINE):
        regex_result = re.search(_REGEX, text)
        # regex_result = re.search(ur'([$])(^\s*)((?:0|[1-9]\d{0,3}(?:,?\d{3})*)(?:\.\d+)?)', text)
        value = text[match.start() : match.end()]
        # Extract an excerpt of text that precedes the match

        # TODO tune this logic
        found_symbol = contains_currency_symbol(match, text)
        found_transaction = contains_transaction_key(match, text)

        # TODO should NOT be OR here -- but get it working for now
        # if found_symbol[0] or found_transaction[0]:
        #     print "FOUND<=================================================="
        # if found_symbol[0]:
        #     print u"<%s> <===CURRECY FOUND===> <%s> "%(value, found_symbol[1].replace(u"\n",u" "))
        # elif found_transaction[0]:
        #     print u"<%s> <===TRANS FOUND===> <%s> "%(value, found_transaction[1].replace(u"\n",u" "))
        # else:
        #     print u"<%s> <XX=NOT CURRECY=XX> <%s> "%(value, found_transaction[1].replace(u"\n",u" "))

        if found_symbol[0] or found_transaction[0]:
            c = {}
            if found_symbol[0]:
                c["symbol_ex"] = found_symbol[1]
            if found_transaction[0]:
                c["trans_ex"] = found_transaction[1]
            if c:
                c["value"] = value
                c["start"] = match.start()
                c["end"] = match.end()
                tagged_currency_entities.append(c)

    return tagged_currency_entities

def scan_str_for_currency_symbols(symbols, full_text_str):
    # symbol = '$'
    text = full_text_str
    total = 0
    while(True):
        # regex_result = re.search(ur'([$])(\d+(?:\.\d{2})?)', text)

        # Currently working regex (without spaces between $ and digits):
        regex_result = re.search(_REGEX, text)
        # regex_result = re.search(ur'([$])(^\s*)((?:0|[1-9]\d{0,3}(?:,?\d{3})*)(?:\.\d+)?)', text)

        print regex_result
        # regex_result = re.search(ur'(\d+(?:\.\d{2})?)([*symbols])', text)
        if(regex_result == None):
            break
        start_ind = regex_result.start()
        groups = regex_result.groups()
        print groups
        print start_ind
        print groups[1]
        total += float(groups[1].replace(',', ''))
        text = text[start_ind + 1:]
    return total

def process_email(email):
    all_the_text = email.get("body", "") +" "+email.get("subject", "") + " " + " ".join(attch.get("contents","") for attch in email["attachments"])
    currency_entities = currency(all_the_text)
    # TODO extract attachment numbers
    email["currency_entities"] = currency_entities
    # TODO uncomment after mapping is migrated to NESTED type
    # for currency_entity in currency_entities:
    #     try:
    #         email["numbers"].append({
    #                              "normalized" : currency_entity["value"],
    #                              "type" : "tansaction" if "trans_ex" in currency_entity else "currency",
    #                              "excerpt" : currency_entity.get("trans_ex", currency_entity.get("symbol_ex",'')),
    #                              "offset_start": currency_entity["start"],
    #                              "offset_end": currency_entity["end"],
    #                          })
    #     except:
    #         print sys.exc_info()[0]
    #         print "Failed to parse currency entity: %s"% currency_entity

    return email

def process_patition(emails):
    for email in emails:
        yield process_email(email)


def test():
    return """
001-001 HK$32"
USD ABC145 $-321
SOME annoying symbol £7510.21 for money
I have sent $200.10 to you. That is 10 $-20.
Subtotal $1000
Total $6000
1000.23
Deposit        $15,000.05

Withdraw       2,000,000,000.00
We have 2,000 lbs of limestone.
When will the 1,000,000.00 USD arrive?
Until then, we will rely on the 20,000 US$ we have remaining.
"""

if __name__ == '__main__':
    # gen_currency_keys()
    # print  currency(test())
    # print "DONE."
    #
    # print currency_key_list
    #
    # if "£" in currency_key_list:
    #     print "In list"
    #
    # print currency_key_list[10]
    # if '£' == (currency_key_list[10]):
    #     print 'match'

    desc='currency extraction'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)


    # SPARK

    parser.add_argument("input_content_path", help="input email or attachment content path")
    parser.add_argument("output_content_currency", help="output text body enriched with currency tags and possibly text locations.")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "INFO: Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)

    conf = SparkConf().setAppName("Newman extract currency")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_content_path).filter(filter_fn).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(process_patition).map(dump).saveAsTextFile(args.output_content_currency)

