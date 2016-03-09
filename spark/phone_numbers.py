# Standard lib
import os, re, glob, json, shutil
import argparse
import json

# 3rd-party modules
import phonenumbers # https://github.com/daviddrysdale/python-phonenumbers
from phonenumbers import geocoder, Leniency
from phonenumbers import carrier
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
phonenum_candidate_regex_str = "([+]?[0-9]?[0-9]?[0-9- .()]{6,15})"

# phonenum_candidate_regex_str = "^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"

entity_descr = "phone numbers"
EXCERPT_CHAR_BUFFER = 50

def find_phone_numbers(source_txt):
    # logger = get_logger()
    tagged_phone_entities = []

    for match in re.finditer(phonenum_candidate_regex_str, source_txt, re.MULTILINE):

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



def contains_tel_keyword(sample_str):
    sample_str = sample_str.lower()
    tel_keyword_list = ['call', 'tel', 'cel', 'mobi', 'landline', 'desk', 'office', 'home', 'work', 'phone', 'fax']

    # If the sample string contains *any* of the keywords return true
    if any(tel_keyword in sample_str for tel_keyword in tel_keyword_list):
        return True

    return False

def process_email(email):
    if "body" in email:
        phones = find_phone_numbers(email["body"])
        # TODO extract attachment numbers
        email["phone_numbers"] = [phone["value_normalized"] for phone in phones]
    return email

def process_patition(emails):
    for email in emails:
        yield process_email(email)

def test():
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
    #
    parser.add_argument("input_content_path", help="input email or attachment content path")
    parser.add_argument("output_content_with_phone_numbers", help="output text body enriched with phone number tags and possibly text locations.")
    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman extract phone numbers")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_content_path).coalesce(50).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(lambda docs: process_patition(docs)).map(dump).saveAsTextFile(args.output_content_with_phone_numbers)
