# Standard lib
import os, re, glob, json, shutil
import argparse
import json
import datetime

# 3rd-party modules
import phonenumbers # https://github.com/daviddrysdale/python-phonenumbers
from phonenumbers import geocoder, Leniency
from phonenumbers import carrier
from operator import attrgetter, itemgetter
from functools import partial
from filters import valid_json_filter
from pyspark import SparkContext, SparkConf


def dump(x):
    return json.dumps(x)


# Regex designed to find phone number candidates (i.e., patterns of at least seven digits,
# possibly separated by a parenthesis, space, period, or hyphen). For more info on the ways phone
# numbers are formatted throughout the world, see
# http://en.wikipedia.org/wiki/National_conventions_for_writing_telephone_numbers
#
# IMPORTANT: Keep this regex compatible with ElasticSearch! For example, don't use \d for digits
# because ES doesn't support it. For more info on ElasticSearch's regex syntax support see
# http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
#
# Regex breakdown:
#
#   # Zero or one instance of a plus or an open parenthesis
#   [+(]?
#
#   # followed by 6-12 instances of this group: a digit followed by one of the allowed separators.
#   # Keep in mind that AFTER this pattern we are matching a single digit, so the overall effect of
#   # 6-12 instances of this group PLUS the digit means SEVEN digits "555-5555" to THIRTEEN digits
#   # (e.g., 3-digit country code plus the standard 10-digit number, like +123 456-789-0123).
#   (
#       [0-9]       # A digit (ElasticSearch doesn't support '\d' for digit)
#       [- .()]?    # followed by zero or one of a hyphen, period, parentheses, or space
#   ){6,12}
#
#   # followed by a digit
#   [0-9]
# Original
# phonenum_candidate_regex_str = "[+(]?([0-9][- .()]?){6,12}[0-9]"
# TODO if updating regex also update the regex in Newman UI es_queries
phone_number_candidate_regex_str = "([+]?[0-9]?[0-9]?[0-9- .()]{6,15})"
creditcard_number_candidate_regex_str = "([0-9]{4}\W*){4}"

# phonenum_candidate_regex_str = "^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"

entity_descr = "phone numbers"
EXCERPT_CHAR_BUFFER = 50

def contains_tel_keyword(sample_str):
    sample_str = sample_str.lower()
    tel_keyword_list = ['call', 'tel', 'cel', 'mob', 'line', 'desk', 'office', 'home', 'work', 'phone', 'fax']

    # If the sample string contains *any* of the keywords return true
    if any(tel_keyword in sample_str for tel_keyword in tel_keyword_list):
        return True

    return False

def find_phone_numbers(source_txt):
    # logger = get_logger()
    tagged_phone_entities = []

    for match in re.finditer(phone_number_candidate_regex_str, source_txt, re.MULTILINE | re.UNICODE):
        # Extract the full-text value and the "normalized" value (i.e., digits only)
        value = source_txt[match.start() : match.end()]
        value_normalized = re.sub(u'[^\d]', u'', value)

        # Extract an excerpt of text that precedes the match
        excerpt_start = match.start() - EXCERPT_CHAR_BUFFER
        if excerpt_start < 0:
            excerpt_start = 0
        excerpt_prefix = source_txt[ excerpt_start : match.start() ]

        phone_number_obj = None

        try:
            # print "phone guess: %s"%value_normalized
            # Try using the 'phonenumbers' module to parse the number. Note that we need to prefix
            # the phone number string with "+" for parse() to work properly. If the number can't
            # be parsed it will throw a NumberParseException.
            phone_number_obj = phonenumbers.parse(u'+'+value_normalized, None)

            # More lenient than valid_num
            possible_num = phonenumbers.is_possible_number(phone_number_obj)
            valid_num= phonenumbers.is_valid_number(phone_number_obj)

            # print "possible=%s valid=%s"%(str(possible_num), str(valid_num))
            # If the phonenumbers module thinks the number is invalid BUT it is preceded by text
            # with a phone-related keyword, we'll accept the number. If the number is invalid and
            # doesn't have a phone-related keyword, however, skip it.
            if (not possible_num or not valid_num) and not contains_tel_keyword(excerpt_prefix[-15:]):
                continue

        except phonenumbers.phonenumberutil.NumberParseException as err:

            # The phonenumbers modules couldn't parse the number; however, if it's preceded by text
            # with a phone-related keyword we'll still accept it. Or put another way, if it is NOT
            # preceded by a phone keyword, skip it.
            if not contains_tel_keyword(excerpt_prefix[-15:]):
                continue

        #There seems to be a bug with some strings that cause partial numbers to be returned
        if len(value_normalized) < 7:
            continue

        # If we got this far, it means we're accepting the number as a phone number. Extract a snippet
        # of text that follows the match.
        excerpt_stop = match.end() + EXCERPT_CHAR_BUFFER
        if excerpt_stop > len(source_txt):
            excerpt_stop = len(source_txt)

        excerpt = source_txt[excerpt_start:excerpt_stop]
        excerpt_value_start = match.start() - excerpt_start
        excerpt_value_stop = excerpt_stop - match.end()

        # Remove carriage returns replace multiple, consecutive whitespace with a single space
        # so the excerpt will be compact and one line.
        excerpt = re.sub('\r+', u'', excerpt)
        excerpt = re.sub('\n+', u' ', excerpt)
        excerpt = re.sub(u'\s+', u' ', excerpt)

        print(u"Phone #: %s, \"%s\"" % (value_normalized, excerpt))

        entity_dict = {
            u"type": u"phone_number",
            u"value": value,
            u"value_normalized": value_normalized,
            u"note": None,
            u"body_offset_start": match.start(),
            u"body_offset_stop": match.end(),
            u"excerpt": excerpt,
            u"excerpt_value_start": excerpt_value_start,
            u"excerpt_value_stop": excerpt_value_stop,
            u"possible_area": None,
            u"possible_carrier": None
        }

        # If the phonenumbers module was able to construct an actual phone number object, attempt to
        # add some notes about the possible geographic region and telco carrier.
        if phone_number_obj is not None:
            area_name = geocoder.description_for_number(phone_number_obj, "en")
            if area_name:
                entity_dict[u'possible_area'] = u"Possible area: %s. " % area_name

            carrier_name = carrier.name_for_number(phone_number_obj, "en")
            if carrier_name:
                entity_dict[u'possible_carrier'] = u"Possible carrier: %s." % carrier_name

        tagged_phone_entities.append(entity_dict)

    return tagged_phone_entities

def find_creditcard_numbers(source_txt):
    # logger = get_logger()
    tagged_phone_entities = []

    for match in re.finditer(creditcard_number_candidate_regex_str , source_txt, re.MULTILINE | re.UNICODE):
        # Extract the full-text value and the "normalized" value (i.e., digits only)
        value = source_txt[match.start() : match.end()]
        value_normalized = re.sub(u'[^\d]', u'', value)

        # Extract an excerpt of text that precedes the match
        excerpt_start = match.start() - EXCERPT_CHAR_BUFFER
        if excerpt_start < 0:
            excerpt_start = 0
        excerpt_prefix = source_txt[ excerpt_start : match.start() ]

        card_number_object = None

        # If we got this far, it means we're accepting the number as a phone number. Extract a snippet
        # of text that follows the match.
        excerpt_stop = match.end() + EXCERPT_CHAR_BUFFER
        if excerpt_stop > len(source_txt):
            excerpt_stop = len(source_txt)

        excerpt = source_txt[excerpt_start:excerpt_stop]
        excerpt_value_start = match.start() - excerpt_start
        excerpt_value_stop = excerpt_stop - match.end()

        # Remove carriage returns replace multiple, consecutive whitespace with a single space
        # so the excerpt will be compact and one line.
        excerpt = re.sub('\r+', u'', excerpt)
        excerpt = re.sub('\n+', u' ', excerpt)
        excerpt = re.sub(u'\s+', u' ', excerpt)

        print(u"Card#: %s, \"%s\"" % (value_normalized, excerpt))

        entity_dict = {
            u"type": u"credit_card_number",
            u"value": value,
            u"value_normalized": value_normalized,
            u"note": None,
            u"body_offset_start": match.start(),
            u"body_offset_stop": match.end(),
            u"excerpt": excerpt,
            u"excerpt_value_start": excerpt_value_start,
            u"excerpt_value_stop": excerpt_value_stop,
            u"possible_area": None,
            u"possible_carrier": None
        }

        tagged_phone_entities.append(entity_dict)

    return tagged_phone_entities

def process_email(email):
    doc = {}
    doc['id'] = email['id']
    email["numbers"] = []
    # TODO remove
    email["phone_numbers"] = []

    numbers = []

    try:
        if "body" in email:
            try:
                numbers += find_phone_numbers(email["body"])
            except:
                print "Failed to process find_phone_numbers email['body'] {}".format(doc['id'])

            try:
                numbers += find_creditcard_numbers(email["body"])
            except:
                print "Failed to process find_creditcard_numbers email['body'] {}".format(doc['id'])


        if "attachments" in email and "content" in email["attachments"]:
            try:
                numbers += find_phone_numbers(email["attachments"]["content"])
            except:
                print "Failed to process find_phone_numbers email['attachments']['content'] {}".format(doc['id'])

            try:
                numbers += find_creditcard_numbers(email["attachments"]["content"])
            except:
                print "Failed to process find_creditcard_numbers email['attachments']['content'] {}".format(doc['id'])


        # TODO remove this field
        email["phone_numbers"] += [num["value_normalized"] for num in numbers]

        email["numbers"] += [{
                                 "normalized" : num["value_normalized"],
                                 "type" : num["type"],
                                 "excerpt" : num["excerpt"],
                             }
                             for num in numbers]

    except:
        print "Failed to process email {}".format(doc['id'])
    return email

def process_patition(emails):
    for email in emails:
        yield process_email(email)

def test_phone():
    print find_phone_numbers( "PHONE: 1021-34662020/21/22/23/24")
    print find_phone_numbers( "1021-34662020")
    print "done.."
    text = "Call me at ++1510-748-8230 if it's before 9:30, or on +703-4800500 after 10am. +971-9-4662020"
    for match in phonenumbers.PhoneNumberMatcher(text, "US"):
        print match

        # text = "PHONE: +971-9-4662020"
        # for match in phonenumbers.PhoneNumberMatcher(text, None, leniency=Leniency.VALID):
        #     print match
        # print geocoder.description_for_number(match, "en")

def test_credit():
    print find_creditcard_numbers( "visa: 1234 4567 1234 5678")
    print find_creditcard_numbers( "visa: 1234-4567-1234-5678")
    print find_creditcard_numbers( "visa: 1234 45671234 5678 foo")
    print find_creditcard_numbers( "visa: 1234 4567 1234 5678foo bar")
    print find_creditcard_numbers( "visa: 1234-4567-1234-5678-0987-0954")
    print find_creditcard_numbers( "visa: 1234\t4567\t1234\t5678foo bar")

if __name__ == '__main__':
    desc='regexp extraction'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)


    # TEST
    # test_credit()
    # test()
    # print "done"

    # SPARK
    #
    parser.add_argument("input_content_path", help="input email or attachment content path")
    parser.add_argument("output_content_with_numbers", help="output text body enriched with phone number and creditcard numbers tags and possibly text locations.")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman extract numbers")
    sc = SparkContext(conf=conf)

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)

    rdd_emails = sc.textFile(args.input_content_path).filter(filter_fn).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(lambda docs: process_patition(docs)).map(dump).saveAsTextFile(args.output_content_with_numbers)
