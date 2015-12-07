#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-

import argparse
import csv

from elasticsearch import Elasticsearch

def get_aggs(address):
    return {
  "aggs": {
    "sent_agg": {
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "datetime": {
                  "gte": "1970-01-01",
                  "lte": "now"
                }
              }
            }
          ],
          "should": [
            {
              "terms": {
                "senders": [address]
              }
            }
          ]
        }
      },
      "aggs": {
        "emails_over_time": {
          "date_histogram": {
            "field": "datetime",
            "interval": "week",
            "extended_bounds": {
              "max": "now",
              "min": "1970-01-01"
            },
            "min_doc_count": 0,
            "format": "yyyy-MM-dd"
          }
        }
      }
    },
    "rcvr_agg": {
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "datetime": {
                  "gte": "1970-01-01",
                  "lte": "now"
                }
              }
            }
          ],
          "should": [
            {
              "terms": {
                "tos": [address]
              }
            },
            {
              "terms": {
                "ccs": [address]
              }
            },
            {
              "terms": {
                "bccs": [address]
              }
            }
          ]
        }
      },
      "aggs": {
        "emails_over_time": {
          "date_histogram": {
            "field": "datetime",
            "interval": "week",
            "extended_bounds": {
              "max": "now",
              "min": "1970-01-01"
            },
            "min_doc_count": 0,
            "format": "yyyy-MM-dd"
          }
        }
      }
    }
  },
  "size": 0
}




def get_email_activity(index, outfile, address):
    es = Elasticsearch()

    resp = es.search(index=index, doc_type="emails", request_cache="false", body=get_aggs(address))

    rows = [[address, s_r[0]["key_as_string"], s_r[0]["doc_count"], s_r[1]["doc_count"]] for s_r in zip(resp["aggregations"]["sent_agg"]["emails_over_time"]["buckets"],
                                                                             resp["aggregations"]["rcvr_agg"]["emails_over_time"]["buckets"])]
    with open(outfile, 'wb') as sent_rcvd_csvfile:
        csv_file=csv.writer( sent_rcvd_csvfile )

        # Add all rows to attachment csv
        csv_file.writerows (rows)

    return resp

if __name__ == "__main__":
    desc='Export attachments from ElassticSearch.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("index", help="index name")
    parser.add_argument("outfile", help="output tar file, e.g. out.tar")
    parser.add_argument("--email_addr", help="email address to export from", default='')
    parser.add_argument("--start_date", help="Start date to export from in yyyy-MM-dd format, e.g. 20001-10-23")
    parser.add_argument("--end_date", help="End date to export from in yyyy-MM-dd format, e.g. 20001-10-23")


    args = parser.parse_args()
    print args.index
    print args.email_addr

    print args.start_date
    print args.end_date

    r=  get_email_activity(args.index, args.outfile, args.email_addr)
    print "done"