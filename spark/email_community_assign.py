#!/usr/bin/env python

import os
import json
import argparse
from operator import itemgetter 
import datetime
from filters import valid_json_filter
from functools import partial
from pyspark import SparkContext, SparkConf

#utils
def dump(x):
    return json.dumps(x)

def identity(x):
    return x

#fn
def communities_of_emails(email_addr):
    community = email_addr.get('community')
    community_id = email_addr.get('community_id')
    senders = email_addr.get('sender', [])
    recepients = email_addr.get('recepient', [])        
    emails = list(set([x['email_id'] for x in senders + recepients]))
    return [(email_id, (community, community_id)) for email_id in emails]

    
def assign_communities(o):
    email_id, (itr_email, itr_communities) = o
    list_email = list(itr_email)    
    if len(list_email) != 1:
        raise Exception("duplicate emails with same ID {}".format(email_id))
        
    # should always be 1
    email = list_email[0]
    email['communities'] = [{'community': comm, 'community_id': comm_id}
                            for comm, comm_id in list(set(itr_communities))]
    email['communities_count'] = len(email['communities'])
    return email


if __name__ == "__main__":

    desc='newman email community assignment'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("input_path_emails", help="directory with json emails")
    parser.add_argument("input_path_email_address", help="directory with json of email address and communities")
    parser.add_argument("output_path_email_with_communities", help="output directory for spark results appending communities to emails")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "INFO: Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)

    conf = SparkConf().setAppName("Newman email community assignment")
    sc = SparkContext(conf=conf)
    rdd_raw_emails = sc.textFile(args.input_path_emails).cache()
    
    rdd_communities_of_emails = sc.textFile(args.input_path_email_address).filter(filter_fn).map(lambda x: json.loads(x)).flatMap(communities_of_emails)
    #print rdd_communities_of_emails.take(10)
    rdd_emails_with_communities = rdd_raw_emails.filter(filter_fn).map(lambda x: json.loads(x)).keyBy(itemgetter('id')) \
                                                .cogroup(rdd_communities_of_emails) \
                                                .map(assign_communities) 

    rdd_emails_with_communities.map(dump).saveAsTextFile(args.output_path_email_with_communities)
    
    print "complete."
