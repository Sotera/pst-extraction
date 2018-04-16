import argparse
import json
import csv
import operator

from elasticsearch import Elasticsearch


def export_edges(index, file, qs='*'):
    es = Elasticsearch()
    body = {
        "query" : {
            "bool":{
                "must":[
                    {
                        "query_string" : { "query" : qs }
                    },
                    {
                        "filtered": {
                            "query": {"bool":{"must":[{"match_all":{}}]}},
                            "filter": {
                                "bool": {
                                    "must": [ { "exists": { "field": "senders"}}],
                                    "should" :[
                                        { "exists": { "field": "tos"}},
                                        { "exists": { "field": "ccs"}},
                                        { "exists": { "field": "bccs"}}
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },
        "sort":  {}
    }


    def rcvrs(fields={}):
        return fields.get("tos",[]) +fields.get("ccs",[])+fields.get("bccs",[])

    count = es.count(index=index, doc_type="emails", body=body)["count"]
    # TODO add batch processing
    addrs = es.search(index=index, doc_type="emails", size=count, from_=0, fields=["senders", "tos", "ccs", "bccs"], body=body)

    edges = reduce(operator.add, [[{"from":hit["fields"]["senders"][0], "to":rcvr}for rcvr in rcvrs(hit["fields"]) ]for hit in addrs["hits"]["hits"]])

    text_file = open(file, "w")
    [text_file.write(json.dumps(edge)+"\n") for edge in edges]
    text_file.close()

def write_csv(out_file, json_file):
    csv_file=csv.writer( open( out_file,'wb'))
    with open(json_file) as json_data:

        for line in json_data:
            edge = json.loads(line)
            values = edge.values()
            values = [s.encode('utf-8') for s in values]
            csv_file.writerow(values)


if __name__ == "__main__":
    desc='Export edges.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("index", help="index name")
    parser.add_argument("--query_string", help="elasticsearch query_string formatted string", default='*')


    args = parser.parse_args()
    print args.index
    print args.query_string

    json_file = "/tmp/edges.json"
    csv_file = "/tmp/edge.csv"
    export_edges(args.index, json_file, args.query_string)
    print "done json"

    write_csv(out_file=csv_file, json_file=json_file)
    print "done csv"
