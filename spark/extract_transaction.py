import re
import locale
import argparse
import json
import csv
from pyspark import SparkContext, SparkConf
import uuid



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

# This the regex side of the extractor:

def run_regex_extraction(sample_text_str):

    # symbol usage:

    # locales=('en_AG', 'en_AU.utf8', 'en_BW.utf8', 'en_CA.utf8',
    #     'en_DK.utf8', 'en_GB.utf8', 'en_HK.utf8', 'en_IE.utf8', 'en_IN', 'en_NG',
    #     'en_NZ.utf8', 'en_PH.utf8', 'en_SG.utf8', 'en_US.utf8', 'en_ZA.utf8',
    #     'en_ZW.utf8')
    locales =('en_AG', 'en_AU.utf8', 'en_CA.utf8',
              'en_HK.utf8',
              'en_NZ.utf8', 'en_SG.utf8', 'en_US.utf8',
              'en_ZW.utf8')

    # for l in locales:
    #     locale.setlocale(locale.LC_ALL, l)
    #     conv=locale.localeconv()

    # print('{int_curr_symbol} ==> {currency_symbol}'.format(**conv))

    #     char_curr_sequence = conv['int_curr_symbol']
    #     currency_symbol = conv['currency_symbol']
    #
    #     print char_curr_sequence
    #     symbol = '$'
    #     trans_amount_l = scan_str_for_currency_symbols(symbol, sample_text_str)
    #     print trans_amount_l
    #
    # print sample_text_str

    symbol = '$'
    trans_amount_l = scan_str_for_currency_symbols(symbol, sample_text_str)
    print trans_amount_l
    return trans_amount_l

# Currently working regex (without spaces between $ and digits):
_REGEX = ur'((?:0|[1-9]\d{0,3}(?:,?\d{3})*)(?:\.\d+)?)'
# _REGEX_ORIG = ur'([$])((?:0|[1-9]\d{0,3}(?:,?\d{3})*)(?:\.\d+)?)'

CURRENCY_CHAR_WINDOW = 6
TRANSACTION_CHAR_WINDOW = 25
# TODO do something better
transaction_keyword_list = ['balance', 'paid','deposit', 'withdraw']
# TODO LOAD from localization api etc
currency_key_list = ['$',"usd","us$"]

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
        return (True, excerpt)

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
    if any(currency_key in excerpt.lower() for currency_key in currency_key_list):
        return (True, excerpt)

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
        if found_symbol[0]:
            print "<%s> <===CURRECY FOUND===> <%s> "%(value, found_symbol[1].replace("\n"," "))
        elif found_transaction[0]:
            print "<%s> <===TRANS FOUND===> <%s> "%(value, found_transaction[1].replace("\n"," "))
        else:
            print "<%s> <XX=NOT CURRECY=XX> <%s> "%(value, found_transaction[1].replace("\n"," "))

        if found_symbol[0] or found_transaction[0]:
            tagged_currency_entities.append({"value" : value, "symbol_ex" : str(found_symbol), "trans_ex" : str(found_transaction)})

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
    return email

def process_patition(emails):
    for email in emails:
        yield process_email(email)


def test():
    return """I have sent $200.10 to you. That is 10 $-20.
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
    # currency(test())
    # print "DONE."


    desc='currency extraction'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)


    # SPARK
    #
    parser.add_argument("input_content_path", help="input email or attachment content path")
    # parser.add_argument("output_content_currency", help="output text body enriched with currency tags and possibly text locations.")
    parser.add_argument("output_csv", help="output attachments to lfs", default="tmp/currency.csv")

    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman extract currency")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_content_path).coalesce(50).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(lambda docs: process_patition(docs)).coalesce(4).foreachPartition(make_csv)
