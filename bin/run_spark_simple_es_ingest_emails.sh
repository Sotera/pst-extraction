#!/usr/bin/env bash

#TODO
# -XDELETE fails because the ReST call has been removed from elastic search 2.x we need to replace with a check to somethig like
# curl -XHEAD -i 'http://localhost:9200/sample/emails'
# which will respond with 200 or 404 accordingly
# TODO

set +x
set -e

if [[ $# -lt 1 ]]; then
    printf "missing configuration\n"
    exit 1
fi

source $1


response=$(curl -XHEAD -i --write-out %{http_code} --silent --output /dev/null "${ES_HOST}:${ES_PORT}/${ES_INDEX}")

if [[ "$response" -eq 404 ]]; then
    printf "create index ${ES_INDEX}\n"
    curl -s -XPUT "${ES_HOST}:${ES_PORT}/${ES_INDEX}" --data-binary "@etc/settings.json"    
fi

response=$(curl -XHEAD -i --write-out %{http_code} --silent --output /dev/null "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_EMAILS}")
if [[ "$response" -eq 200 ]]; then
    printf "delete doc_type\n"
    curl -XDELETE "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_EMAILS}"
fi

printf "create emails doc_type\n"
curl -s -XPUT "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_EMAILS}/_mapping" --data-binary "@etc/emails.mapping"


response=$(curl -XHEAD -i --write-out %{http_code} --silent --output /dev/null "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_CLUSTERING}")
if [[ "$response" -eq 200 ]]; then
    printf "delete doc_type\n"
    curl -XDELETE "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_CLUSTERING}"
fi

printf "create lda-clustering doc_type\n"
curl -s -XPUT "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_CLUSTERING}/_mapping" --data-binary "@etc/lda-clustering.mapping"

printf "ingest lda clusters\n"
./src/upload_lda_clusters.py ${ES_INDEX} --es_nodes ${ES_NODES}

printf "ingest entity documents\n"

spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.2.0-m1.jar --conf spark.storage.memoryFraction=.8 spark/es_simple_ingest.py "pst-extract/spark-emails-entity/part-*" "${ES_INDEX}/${ES_DOC_TYPE_EMAILS}" --id_field id  --es_nodes ${ES_NODES}
