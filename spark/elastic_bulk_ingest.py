from pyspark import SparkContext, SparkConf

import json
import argparse

def fn_to_doc(line):
    try:
        doc = {}
        data = json.loads(line)
        doc['data'] = data
        return [json.dumps(doc)]
    except:
        return []

if __name__ == "__main__":

    desc='elastic search ingest'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("input_path", help="lines of json to ingest")
    parser.add_argument("es_resource", help="index and doc_type (my-index/doc)")    
    parser.add_argument("--id_field", help="id field to map into es")    
    parser.add_argument("--es_nodes", default="127.0.0.1", help="es.nodes")
    parser.add_argument("--es_port", default="9200", help="es.port")    

    args = parser.parse_args()

    conf = SparkConf().setAppName("Elastic Ingest")
    sc = SparkContext(conf=conf)

    es_write_conf = {
        "es.nodes" : args.es_nodes,
        "es.port" : args.es_port,
        "es.resource" : args.es_resource,
        #"es.nodes.client.only" : "true",
        "es.input.json" : "yes"
    }
    
    if args.id_field:
      es_write_conf["es.mapping.id"] = args.id_field

    hdfs_path = args.input_path
    d = sc.textFile(hdfs_path).map(lambda x : ("key", x))

    d.saveAsNewAPIHadoopFile(
        path='-', 
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.apache.hadoop.io.Text",
        #valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=es_write_conf)

