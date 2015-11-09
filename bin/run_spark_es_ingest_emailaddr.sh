#!/usr/bin/env bash

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

response=$(curl -XHEAD -i --write-out %{http_code} --silent --output /dev/null "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_EMAILADDR}")
if [[ "$response" -eq 200 ]]; then
    printf "delete doc_type\n"
    curl -XDELETE "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_EMAILADDR}"
fi

printf "create doc_type\n"
curl -s -XPUT "${ES_HOST}:${ES_PORT}/${ES_INDEX}/${ES_DOC_TYPE_EMAILADDR}/_mapping" --data-binary "@etc/email_address.mapping"

printf "ingest documents\n"

spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.2.0-m1.jar --conf spark.storage.memoryFraction=.8 spark/elastic_bulk_ingest.py "pst-extract/spark-emailaddr/part-*" "${ES_INDEX}/${ES_DOC_TYPE_EMAILADDR}"
