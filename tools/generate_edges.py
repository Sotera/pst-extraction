import json
import csv
import operator

from elasticsearch import Elasticsearch


def export_edges(index, file):
    es = Elasticsearch()
    body = {
        "query": {
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
            csv_file.writerow( edge.values() )

if __name__ == "__main__":
    json_file = "/tmp/edges.json"
    csv_file = "/tmp/edge.csv"
    export_edges("sample", json_file)
    print "done json"

    write_csv(out_file=csv_file, json_file=json_file)
    print "done csv"
