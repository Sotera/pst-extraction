#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import re
import argparse
import json
from functools import partial
from operator import attrgetter, itemgetter
from pyspark import SparkContext, SparkConf
import geoip2.errors

#
# This code requires goe mmdb available on the localhost
# This comes pre-installed with the geo-utils docker image
#
# It can be downloaded at:
# http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz
#
#

def loc(reader, ip):
    try:
        response = reader.city(ip)
        name, lat, lon = attrgetter("city.name", "location.latitude", "location.longitude")(response)
        rtn = {"city": name, "geo_coord" : {"lat": lat, "lon": lon}}
        return (True, rtn)
    except (ValueError, geoip2.errors.AddressNotFoundError) as e:
        return (False, str(e))

def assign_ips(accum_count, geodb_path, partition):
    import geoip2.database            
    items = []
    reader = None
    try:
        reader = geoip2.database.Reader(geodb_path)                
        for item in partition:
            email_locs = []
            ips = [ip.replace("[","").replace("]","") for ip in item.get("originating_ips", [])]
            for ip in ips:
                op_loc = loc(reader, ip)
                if op_loc[0]:
                    email_locs.append(op_loc[1])
                    accum_count.add(1)
            items.append(dict(item, originating_locations=email_locs))
    finally:
        if reader:
            reader.close()
    return iter(items)

def dump(x):
    return json.dumps(x)

if __name__ == "__main__":
    desc='Extract locations from IP address'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("-d", "--geodb", default="etc/GeoLite2-City.mmdb", help="path to ip2geo db")        
    parser.add_argument("input_emails_content_path", help="email json")
    parser.add_argument("output_emails_with_ip_assignment", help="output directory for emails with ip geo located")

    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman assign ips to emails")
    sc = SparkContext(conf=conf)
    ips_count = sc.accumulator(0)
    mapfn = partial(assign_ips, ips_count, args.geodb)
    rdd_emails = sc.textFile(args.input_emails_content_path).coalesce(50).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(mapfn).map(dump).saveAsTextFile(args.output_emails_with_ip_assignment)

    print "IP Locations extracted {} ".format(ips_count.value)
    print "complete."
