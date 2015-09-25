#!/usr/bin/env python
from pyspark import SparkContext, SparkConf

import sys, os
import json
import argparse
from operator import itemgetter 

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
    
    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman email community assignment")
    sc = SparkContext(conf=conf)
    rdd_raw_emails = sc.textFile(args.input_path_emails).cache()
    
    rdd_communities_of_emails = sc.textFile(args.input_path_email_address).map(lambda x: json.loads(x)).flatMap(communities_of_emails)
    #print rdd_communities_of_emails.take(10)
    rdd_emails_with_communities = rdd_raw_emails.map(lambda x: json.loads(x)).keyBy(itemgetter('id')) \
                                                .cogroup(rdd_communities_of_emails) \
                                                .map(assign_communities) 

    rdd_emails_with_communities.map(dump).saveAsTextFile(args.output_path_email_with_communities)
    
    print "complete."
