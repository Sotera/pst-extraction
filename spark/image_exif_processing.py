import sys
import exifread
import base64
import cStringIO

import argparse
import json
import os
import datetime
from filters import valid_json_filter
from functools import partial
from pyspark import SparkContext, SparkConf


def dump(x):
    return json.dumps(x)


def string_to_dms(string_dms):
    d_str, m_str, s_str= string_dms.split(",")
    def convert_str(str_val):
        stringD = str_val.split("/", 2)
        return float(stringD[0])  if len(stringD) == 1 else float(stringD[0]) / float(stringD[1])

    deg = convert_str(d_str)
    min = convert_str(m_str)
    sec = convert_str(s_str)

    return float(deg + (min/60) + (sec/3600))

def ratio_to_dms(dms_ratio):
    def frac_to_dec(ratio):
        return float(ratio.num) / float(ratio.den)

    return frac_to_dec(dms_ratio[0]) + (frac_to_dec(dms_ratio[1])/60.0) + (frac_to_dec(dms_ratio[2])/3600.0)

def process_image(byte64_jpeg, filename):

    attch_data = str(base64.b64decode(byte64_jpeg))
    buf = cStringIO.StringIO(attch_data)

    gps = {}
    try:
        tags = exifread.process_file(buf)
    except:
        print "failed to process tags for filename"%filename, sys.exc_info()[0]
        return gps

    if tags and len(tags) > 0:
        if "GPS GPSLatitudeRef" in tags:
            gps["latref"] = tags["GPS GPSLatitudeRef"].printable
        if "GPS GPSLongitudeRef" in tags:
            gps["lonref"] = tags["GPS GPSLongitudeRef"].printable
        if "GPS GPSAltitudeRef" in tags:
            gps["altref"] = tags["GPS GPSAltitudeRef"].printable

        if "GPS GPSLatitude" in tags:
            gps["lat"] = (1 if "latref" in gps and gps["latref"] == "N" else -1 ) * ratio_to_dms(tags["GPS GPSLatitude"].values)


        if "GPS GPSLongitude" in tags:
            gps["lon"] = (1 if "lonref" in gps and gps["lonref"] == "E" else -1 ) * ratio_to_dms(tags["GPS GPSLongitude"].values)

        if "GPS GPSAltitude" in tags:
            gps["alt"] = tags["GPS GPSAltitude"].printable

    else:
        print "No exif gps tags for: %s "%filename

    # TODO remove
    print "File: %s, gps:%s"%(filename,str(gps))

    return gps

# Validate lat lon
def validate(gps, filename):
    # TODO need better logic for this validation Should not be set to 0
    if "lat" in gps and (gps["lat"] > 90 or gps["lat"] < -90):
        print "ERROR: EXIF Invalid latitude in file %s, gps:%s"%(filename, str(gps))
        gps["lat"] = 0
    if "lon" in gps and (gps["lon"] > 180 or gps["lon"] < -180):
        print "ERROR: EXIF Invalid longitude in file %s, gps:%s"%(filename, str(gps))
        gps["lon"] = 0
    return gps

def process_attachment(attachment):
    if attachment:
        attachment["exif"] = {}
        if "contents64" in attachment and "extension" in attachment and (attachment["extension"] == ".jpg" or attachment["extension"] == ".jpeg"):
            gps={}
            try:
                gps = process_image(attachment["contents64"], attachment["filename"])
                # TODO fix this
                gps = validate(gps, attachment["filename"])
                if gps:
                    attachment["exif"]["gps"] = gps
                    attachment["exif"]["gps"]["coord"] = {"lat":float(gps["lat"]), "lon":float(gps["lon"])}
                    gps.pop('lat', -1)
                    gps.pop('lon', -1)
            except:
                print "FAILED:  File: %s, gps:%s"%(attachment["filename"],str(gps))
                print "ERROR:", sys.exc_info()[0]

    return attachment

def process_email(email):
    for attachment in email["attachments"]:
        process_attachment(attachment)
    return email

def read_image_as_byte64(file="/home/elliot/images_set/img-20110513-00784.jpg"):
    with open(file, 'rb') as data_file:
        data = base64.b64encode(data_file.read())
        return data


def read_json_file():
    with open('cresentful.json') as data_file:
        data = json.load(data_file)
        return data

if __name__ == '__main__':
    desc='exif extract'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)


    # TEST
    # json_attachment = read_json_file()
    # process_image(json_attachment["contents64"])
    # file_with_with_gps="/home/elliot/images_set/img-20110513-00784.jpg"
    # file_with_no_gps="/home/elliot/images_set/dcoo0080.jpg"
    # print process_image(read_image_as_byte64(file_with_no_gps), "testfile.jpg")
    # print process_image(read_image_as_byte64(file_with_with_gps), "testfile.jpg")
    # print process_image(read_image_as_byte64("cresentful.json"), "testfile.jpg")

    # SPARK

    parser.add_argument("input_attachment_content_path", help="input attachments")
    parser.add_argument("output_attachments_gps", help="output attachments enriched with geo / gps tagged images when applicable")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")
    args = parser.parse_args()

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)


    conf = SparkConf().setAppName("Newman extract jpeg exif ")
    sc = SparkContext(conf=conf)
    rdd_emails = sc.textFile(args.input_attachment_content_path).filter(filter_fn).map(lambda x: json.loads(x))
    rdd_emails.map(lambda email : process_email(email)).map(dump).saveAsTextFile(args.output_attachments_gps)

    # # TODO need path for handling emails and stand alone attachments
    # rdd_emails.map(lambda x : process_attachment(x)).filter(lambda x : "gps" in x["exif"]).map(dump).saveAsTextFile(args.output_attachments_gps)
