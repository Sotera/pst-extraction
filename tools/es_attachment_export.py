import argparse
import base64
import cStringIO
import tarfile
import time
import json
import csv

import tangelo
import cherrypy
from elasticsearch import Elasticsearch


def attch_ext_query(extension):
    return {
        "query": {
            "filtered": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_all": {}
                            }
                        ]
                    }
                },
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "sender_attachments.extension": extension
                                }
                            }
                        ],
                        "should": [
                        ]
                    }
                }
            }
        }
    }

# Query by nested object -- response will return the parent hits along with the inner_hits collection of nested documents
# Needed for retrieving the attachments that matched the search criteria
def attch_nested__ext_query(extension):
    return {
        "query" : {
            "nested" : {
                "path" : "sender_attachments",
                "query" : {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "sender_attachments.extension": extension
                                }
                            }
                        ],
                        "should": [
                        ]
                    }
                },
                "inner_hits" : {}
            }
        }
    }

def _attch_nested__ext_query(address=None, extension=None, date_bounds=None,max_inner_attachments_returned=3):

    must=[]
    must += [] if not extension else [{"term": {"sender_attachments.extension": extension}}]
    must += [] if not date_bounds else [{"range" : {"sender_attachments.datetime" : { "gte": str(date_bounds[0]), "lte": str(date_bounds[1])}}}]

    return {
        "query" : {
            "bool":{
                "must" : [
                    {
                        "filtered": {
                            "query": {
                                "bool": {
                                    "must": [
                                        {
                                            "match_all": {}
                                        }
                                    ]
                                }
                            },
                            "filter": {
                                "bool": {
                                    "must":[] if not address else [{"term": {"addr": address}}],
                                    "should": []
                                }
                            }
                        }
                    },
                    {
                        "nested" : {
                            "path" : "sender_attachments",
                            "query" : {
                                "filtered" : {
                                    "filter" :  {
                                        "bool":{
                                            "should":[],
                                            "must":must,
                                            "must_not":[]
                                        }
                                    }
                                }
                            },
                            "inner_hits" :
                                {
                                    "size" : max_inner_attachments_returned
                                }
                        }
                    }
                ]
            }
        }
    }

def header(h, t=None):
    r = cherrypy.response.headers.get(h)

    if t is not None:
        cherrypy.response.headers[h] = t

    return r

# GET email/attachment/<attachment-GUID>?data_set_id=<data_set>
def export_attachments(es_host, data_set_id, outfile, sender='', attachment_extension='jpg', date_bounds=None):
    print("email.get_attachments_sender(index=%s, sender=%s, attachment_type=%s, date_bounds=%s)" % (data_set_id, sender, attachment_extension, date_bounds))
    if not data_set_id:
        print "invalid service call - missing index"
        return 1
    # elasticsearch.exceptions.ConnectionTimeout: ConnectionTimeout caused by - ReadTimeoutError(HTTPConnectionPool(host='10.1.70.143', port=9200): Read timed out. (read timeout=10))
    es = Elasticsearch([{"host" : es_host, "port" : 9200}], timeout=60)

    # TODO get accurate count -- this is not strictly needed as attachments will be accessed as inner docs on the email_address
    max_inner_attachments_returned = 100000

    # Get all attachments by extension
    rows=[]
    body = _attch_nested__ext_query(sender, attachment_extension, date_bounds, max_inner_attachments_returned=max_inner_attachments_returned )
    print body
    addresses_count = es.count(index=data_set_id, doc_type="email_address", body=body)["count"]
    print "total addresses: " + str(addresses_count)
    addresses = es.search(index=data_set_id, doc_type="email_address", body=body, size=addresses_count)
    for address in addresses["hits"]["hits"]:
        rows += [[address["_source"]["addr"], attachment["_source"]["guid"], attachment["_source"]["filename"], attachment["_source"]["datetime"]] for attachment in address["inner_hits"]["sender_attachments"]["hits"]["hits"]]

    print "total attachments: " + str(len(rows))

    #  start tar.gz
    # tar = tarfile.open(mode='w:gz', name="big-export.tar.gz")
    # Start tar
    tar = tarfile.open(mode='w', name=outfile)

    csv_string_buffer = cStringIO.StringIO()
    csv_file=csv.writer( csv_string_buffer )

    # Add all rows to attachment csv
    csv_file.writerows (rows)
    tarinfo = tarfile.TarInfo("attachments.csv")

    tarinfo.size = csv_string_buffer.tell()
    tarinfo.mode = 0644
    tarinfo.mtime = time.time()
    csv_string_buffer.seek(0)

    tar.addfile(tarinfo, csv_string_buffer)


    # This is the buffer size of how many attachments to pull from ES at each iteration
    num_returned=3
    index=0
    # Paging
    while index < len(rows):
        # Get num_returned attachments from ES
        attachments = es.mget(index=data_set_id, doc_type="attachments", body={"docs":[{"_id":row[1]} for row in rows[index: index+num_returned]]})
        index+=num_returned

        # Add all attachments to the archive
        for attachment_source in attachments["docs"]:
            attachment = attachment_source["_source"]
            filename = attachment["filename"]
            attch_data = str(base64.b64decode(attachment["contents64"]))

            tarinfo_attch = tarfile.TarInfo(attachment["guid"]+"/"+filename)
            tarinfo_attch.size = len(attch_data)
            tarinfo_attch.mode = 0644
            tarinfo_attch.mtime = time.time()
            tar.addfile(tarinfo_attch, cStringIO.StringIO(attch_data))

    tar.close()


# GET email/attachment/<attachment-GUID>?data_set_id=<data_set>
# Build a tar.gz file in memory with all emails and attachment binary and export
def export_emails_archive(data_set_id, email_ids=["f9c9c59a-7fe8-11e5-bb05-08002705cb99"]):
    cherrypy.log("email.get_attachments_sender(index=%s, attachment_id=%s)" % (data_set_id, email_ids))
    if not data_set_id:
        return tangelo.HTTPStatusCode(400, "invalid service call - missing index")
    # if not email:
    #     return tangelo.HTTPStatusCode(400, "invalid service call - missing attachment_id")

    # elasticsearch.exceptions.ConnectionTimeout: ConnectionTimeout caused by - ReadTimeoutError(HTTPConnectionPool(host='10.1.70.143', port=9200): Read timed out. (read timeout=10))
    es = Elasticsearch([{"host" : "10.1.70.143", "port" : 9200}], request_timeout=60)
    # TODO can implement with multiple doc_types and combine attachments in
    emails = es.mget(index=data_set_id, doc_type="emails", body={"docs":[{"_id":id} for id in email_ids]})


    # TODO filename
    filename= "export.tar.gz"
    tangelo.content_type("application/x-gzip")
    header("Content-Disposition", 'attachment; filename="{}"'.format(filename))

    string_buffer = cStringIO.StringIO()
    tar = tarfile.open(mode='w:gz', fileobj=string_buffer)

    # Add each email to the tar
    for email_source in emails["docs"]:

        email = email_source["_source"]

        tarinfo_parent= tarfile.TarInfo(name = email["id"])
        tarinfo_parent.type = tarfile.DIRTYPE
        tarinfo_parent.mode = 0755
        tarinfo_parent.mtime = time.time()
        tar.addfile(tarinfo_parent)

        tarinfo = tarfile.TarInfo(email["id"]+"/"+email["id"]+".json")
        # TODO -- email transformation
        data_string = json.dumps(email)
        fobj = cStringIO.StringIO(data_string)

        tarinfo.size = len(data_string)
        tarinfo.mode = 0644
        tarinfo.mtime = time.time()
        tar.addfile(tarinfo, fobj)

        # Get the attachments
        if email["attachments"]:
            attachments = es.mget(index=data_set_id, doc_type="attachments", body={"docs":[{"_id":attch["guid"]} for attch in email["attachments"]]})
            for attachment_source in attachments["docs"]:
                attachment = attachment_source["_source"]
                filename = attachment["filename"]
                attch_data = str(base64.b64decode(attachment["contents64"]))

                tarinfo_attch = tarfile.TarInfo(email["id"]+"/"+filename)
                tarinfo_attch.size = len(attch_data)
                tarinfo_attch.mode = 0644
                tarinfo_attch.mtime = time.time()
                tar.addfile(tarinfo_attch, cStringIO.StringIO(attch_data))
    tar.close()

    return string_buffer.getvalue()


if __name__ == "__main__":
    desc='Export attachments from ElassticSearch.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("index", help="index name")
    parser.add_argument("outfile", help="output tar file, e.g. out.tar")
    parser.add_argument("--es_host", help="elasticsearch host", default='localhost')
    parser.add_argument("--email_addr", help="email address to export from", default='')
    parser.add_argument("--file_type", help="type of file to export .e.g. jpg, xsl", default='xls')
    parser.add_argument("--start_date", help="Start date to export from in yyyy-MM-dd format, e.g. 20001-10-23")
    parser.add_argument("--end_date", help="End date to export from in yyyy-MM-dd format, e.g. 20001-10-23")


    args = parser.parse_args()
    print args.es_host
    print args.index
    print args.email_addr

    print args.file_type
    print args.start_date
    print args.end_date

    date_bounds = None if not (args.start_date and args.end_date) else (args.start_date, args.end_date)
    export_attachments(args.es_host, args.index, args.outfile,  args.email_addr, args.file_type, date_bounds)

