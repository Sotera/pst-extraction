#!/usr/bin/env python3

import optparse
import sys
import elasticsearch
import requests
import pprint

version="0.0.1"



parser = optparse.OptionParser()
parser.add_option('--verbose',
                  help="enable verbose reporting",
                  action="store_true",
                  default=False,
                  dest="verbose")

parser.add_option('-v',
                  help="display version information",
                  action="store_true",
                  default=False,
                  dest="version")

parser.add_option("--esCluster",
                  help="Name of Elastic Search Cluster",
                  dest="es_cluster",
                  default="localhost")

parser.add_option("--ls",
                  help="list indicies",
                  action="store_true",
                  dest="es_ls",
                  default=False)

parser.add_option("--label",
                  help="Change label of an index.  MUST BE USED with esCluster AND esIndex",
                  dest="es_label")

parser.add_option("--esIndex",
                  help="ES Index to change Newman Label of",
                  dest="es_index")


options, remainder = parser.parse_args()
parser.parse_args()
pp = pprint.PrettyPrinter(indent=4)


def ls_indices():
    """
    Function to list Elasticsearch indices
    Can help to identify what underlying newman-XXXXX index maps to what Newman web UI label

    :return: nothing
    """
    print("Index => Newman Label Map:")
    for idx in es.indices.get('*'):
        res = es.search(index=idx, body={"query":{"bool":{"must":[{"query_string":{"default_field":"_all","query":idx}}],"must_not":[],"should":[]}},"from":0,"size":1,"sort":[],"aggs":{}})
        label = res['hits']['hits'][0]['_source']['label']

        print("\t" + idx + "\t=>\t" + label)
def es_label_update(es_index, label_name):
    """
    Function to update an Elasticsearch label for newman.  This *ONLY* affects the newman web ui front end.

    :param es_index:
    :param label_name:
    :return: nothing
    """

    try:
        es.update(index=es_index,

                  doc_type=es_index,
                  id=es_index,
                  body={"doc": { "label": label_name}})
    except Exception as ex:
        print("Error:  Unable to update Elasticsearch index label.  Please verify that this ES index is a newman index.")
        print(ex)
def print_version_info():
    """
    simple function to print version information.  This is a weak version output and as such it is not indicitive
    of all changes made/not made to this code, but should be seen as a general/rough idea.
    :return:
    """
    print("version is:  " + version)



if __name__ == "__main__":
    if (options.verbose):
        print("verbose is:  " + str(options.verbose))
        print("Elasticsearch module is:  " + str(elasticsearch.__version__))


    # if we get the request for version information, display that then quit -- no need to go on
    if (options.version):
        print_version_info()
        sys.exit(0)


    # Try to make a connection to elasticsearch
    # If elasticsearch wasn't specified it defaults to 'localhost'
    try:
        res = requests.get("http://" + options.es_cluster + ":9200")
        es = elasticsearch.Elasticsearch([{'host': options.es_cluster, 'port': '9200'}])

        if(options.verbose):
            print("request (res) content is:  ")
            pp.pprint(res.content)

            print("elasticsearch connection made!")
            print(str(es))
    except Exception as ex:
        print("Error:  Unable to connect to elasticsearch cluster!")
        print(ex)
        sys.exit(0)


    if (options.es_ls):
        ls_indices()
    elif (options.es_label is not None or options.es_index is not None):
        if (options.es_index is None):
            print("Error:  No elasticsearch index id specified!")
            sys.exit(0)
        if (options.es_label is None):
            print("Error:  Label is not specified")
            sys.exit(0)

        if (options.verbose):
            print("ES Cluster:  " + str(options.es_cluster))
            print("ES Index:  " + str(options.es_index))
            print("ES Label:  " + str(options.es_label))
        try:
            es_label_update(es_index=options.es_index, label_name=options.es_label)
        except Exception as ex:
            print("Error:  Check that you specified a label and index!")
            print(ex)
    else:
        sys.exit(0)
