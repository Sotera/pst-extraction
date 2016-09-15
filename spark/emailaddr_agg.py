#!/usr/bin/env python
import json
import argparse
import igraph
import os
import datetime
from filters import valid_json_filter
from functools import partial
from pyspark import SparkContext, SparkConf

#utils
def split_on_condition(seq, condition):
    truthy, falsy = [], []
    for item in seq:
        (truthy if condition(item) else falsy).append(item)
    return truthy,falsy

def rmkey(k, o):
    if k in o:
        del o[k]
    return o

def rmkeys(keys, o):
    for k in keys:
        if k in o:
            del o[k]
    return o

def extractKeys(keys, o):
    rtn = {}
    for k in keys:
        if k in o:
            rtn[k] = o[k]
    return rtn

def dump(x):
    return json.dumps(x)

def one(arr, default=None):
    return arr[0] if arr else default

def identity(x):
    return x

#fn

##
## lambda {} -> { addr: _, datetime: dt, type: sender|recepient, email_id: id } 
##
def email_to_addrs(o):
    j = json.loads(o)
    email_id = j.get('id')
    email_dt = j.get('datetime')

    removeContent = partial(rmkey, 'content')
    attachments = [dict(attach, email_id=email_id, datetime=email_dt) for attach in map(removeContent, j.get('attachments', []))]
    sender = one(j.get('senders', []))
    tos = j.get('tos', [])
    ccs = j.get('ccs', [])
    bccs = j.get('bccs', [])
    #if datetime is empty string convert it to None
    dt = j.get('datetime', None)
    dt = dt if dt else None 
    # attachments get add to the sender only and empty list for recepients
    recepients = [{'addr': addr, 'datetime': dt, 'type': 'recepient', 'email_id': email_id, 'attachments': [] } for addr in filter(identity, list(set(tos + ccs + bccs)))]
    return ([{'addr' : sender, 'datetime': dt, 'type': 'sender', 'email_id': email_id, 'attachments' : attachments}] if sender else []) + recepients

def in_out(t):
    addr, arr = t
    sender_emails, recepient_emails = split_on_condition(arr, lambda x: x['type'] == 'sender')
    attachments = reduce(lambda a,b: a+b.get('attachments',[]), sender_emails, [])
    return {'addr' : addr,
            'sender_attachments' : attachments,
            'sender' : map(lambda x: rmkeys(['addr', 'type', 'attachments'], x), sender_emails), 
            'recepient': map(lambda x: rmkeys(['addr', 'type', 'attachments'], x), recepient_emails)}

def sender_receiver(o):
    j = json.loads(o)
    sender = one(j.get('senders', []))
    tos = j.get('tos', [])
    ccs = j.get('ccs', [])
    bccs = j.get('bccs', [])
    return [((sender, addr), 1) for addr in list(set(tos + ccs + bccs))]


if __name__ == "__main__":

    desc='newman extract email addresses and build aggregations'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    parser.add_argument("input_path_emails", help="directory with json emails")
    parser.add_argument("output_path_email_address", help="output directory for spark results of json email address ")

    parser.add_argument("-i", "--ingest_id", required=True, help="ingest id, usually the name of the email account, or the ingest process")
    parser.add_argument("-c", "--case_id", required=True, help="case id used to track and search accross multiple cases")
    parser.add_argument("-a", "--alt_ref_id", required=True, help="an alternate id used to corelate to external datasource")
    parser.add_argument("-b", "--label", required=True, help="user defined label for the dateset")
    parser.add_argument("-v", "--validate_json", action="store_true", help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)


    conf = SparkConf().setAppName("Newman email address aggregations")
    sc = SparkContext(conf=conf)
    rdd_raw_emails = sc.textFile(args.input_path_emails).cache()
    rdd_addr_to_emails = rdd_raw_emails.filter(filter_fn).flatMap(email_to_addrs).keyBy(lambda x: x['addr']).groupByKey().map(in_out).cache()
    #rdd_addr_to_emails.saveAsTextFile(args.output_path_email_address)
    
    rdd_edges = rdd_raw_emails.filter(filter_fn).flatMap(sender_receiver).reduceByKey(lambda a,b: a+b).map(lambda x: (x[0][0], x[0][1], x[1])).cache()

    nodes = rdd_edges.flatMap(lambda x: [x[0],x[1]]).distinct().collect()
    node_map = {k: v for v,k in enumerate(nodes)}
    broadcast_node_map = sc.broadcast(node_map)
    nodes_idx = map(lambda x: {'addr' : x, 'idx': node_map[x], 'community': 'n/a'}, nodes)
    edges = rdd_edges.map(lambda x: (broadcast_node_map.value[x[0]], broadcast_node_map.value[x[1]])).collect()

    g = igraph.Graph(len(nodes)+1)
    g.add_edges(edges)
    g.vs['node'] = nodes_idx

    g = g.as_undirected(mode='collapse')
    clustering = g.community_multilevel()

    for subgraph in clustering.subgraphs():
        #pick name of first node for community
        community_name = subgraph.vs['node'][0]['addr']
        for node in subgraph.vs['node']:
            node['community'] = community_name
            node['community_id'] = node_map[community_name]

    #{'community': u'', 'addr': u'', 'idx': 423, 'community_id': 4}
    rdd_communities = sc.parallelize(nodes_idx).keyBy(lambda x: x['addr'])

    def add_meta(o):
        def apply_filter_map(l, apply_fn, pred, map_fn, default=None):
            try:
                return apply_fn(filter(pred, map(map_fn, l)))
            except ValueError:
                return default
                    
        o['sent_count'] = len(o.get('sender', []))
        o['received_count'] = len(o.get('recepient', []))
        o['attachments_count'] = len(o.get('sender_attachments', []))
        o['first_sent'] = apply_filter_map(o.get('sender', []), min, identity, lambda x: x.get('datetime', None))
        o['first_received'] = apply_filter_map(o.get('recepient', []), min, identity, lambda x: x.get('datetime', None))
        o['last_received'] =  apply_filter_map(o.get('recepient', []), max, identity, lambda x: x.get('datetime', None))
        o['last_sent'] = apply_filter_map(o.get('sender', []), max, identity, lambda x: x.get('datetime', None))

        o["ingest_id"] = args.ingest_id
        o["case_id"] = args.case_id
        o["alt_ref_id"] = args.alt_ref_id
        o["label"] = args.label
        return o
    
    rdd_communities_assigned = rdd_addr_to_emails.keyBy(lambda x: x['addr']) \
                                                 .join(rdd_communities) \
                                                 .map(lambda x: dict(x[1][0], **x[1][1])) \
                                                 .map(add_meta).cache()
    
    rdd_communities_assigned.map(dump).saveAsTextFile(args.output_path_email_address)

    print "complete."
