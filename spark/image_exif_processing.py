import sys
import exifread
import base64
import cStringIO

import argparse
import json
from pyspark import SparkContext, SparkConf


def dump(x):
    return json.dumps(x)

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
        if "GPS GPSLatitude" in tags:
            gps["lat"] = tags["GPS GPSLatitude"].printable
        if "GPS GPSLatitudeRef" in tags:
            gps["latref"] = tags["GPS GPSLatitudeRef"].printable
        if "GPS GPSLongitude" in tags:
            gps["lng"] = tags["GPS GPSLongitude"].printable
        if "GPS GPSLongitudeRef" in tags:
            gps["lngref"] = tags["GPS GPSLongitudeRef"].printable
        if "GPS GPSAltitude" in tags:
            gps["alt"] = tags["GPS GPSAltitude"].printable
        if "GPS GPSAltitudeRef" in tags:
            gps["altref"] = tags["GPS GPSAltitudeRef"].printable
    else:
        print "No tags for: %s "%filename

    # TODO remove
    print "File: %s, gps:%s"%(filename,str(gps))

    return gps


def process_attachment(attachment):
    if attachment:
        attachment["locations"] = {}
        if "contents64" in attachment and "extension" in attachment and (attachment["extension"] == ".jpg" or attachment["extension"] == ".jpeg"):
            gps = process_image(attachment["contents64"], attachment["filename"])
            if gps:
                attachment["locations"]["gps"] = gps
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
    #
    parser.add_argument("input_attachment_content_path", help="input attachments")
    parser.add_argument("output_attachments_gps", help="output attachments enriched with geo / gps tagged images when applicable")
    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman extract jpeg exif ")
    sc = SparkContext(conf=conf)
    rdd_emails = sc.textFile(args.input_attachment_content_path).map(lambda x: json.loads(x))
    # TODO need path for handling emails and stand alone attachments
    # rdd_emails.map(lambda x : process_attachment(x)).filter(lambda x : "gps" in x["location"]).map(dump).saveAsTextFile(args.output_attachments_gps)
    rdd_emails.map(lambda email : process_email(email)).map(dump).saveAsTextFile(args.output_attachments_gps)
