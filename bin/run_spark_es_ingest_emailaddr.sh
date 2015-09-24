#!/usr/bin/env bash

set +x
set -e

INDEX=sample
DOC_TYPE=email_address

response=$(curl -XHEAD -i --write-out %{http_code} --silent --output /dev/null "localhost:9200/${INDEX}")

if [[ "$response" -eq 404 ]]; then
    printf "create index ${INDEX}\n"
    curl -s -XPOST "http://localhost:9200/${INDEX}" -d '{  "settings": { "index": { "mapping.allow_type_wrapper": true  }  }  }'    
fi

response=$(curl -XHEAD -i --write-out %{http_code} --silent --output /dev/null "localhost:9200/${INDEX}/${DOC_TYPE}")
if [[ "$response" -eq 200 ]]; then
    printf "delete doc_type\n"
    curl -XDELETE "localhost:9200/${INDEX}/${DOC_TYPE}"
fi

printf "create doc_type\n"
curl -s -XPUT "http://localhost:9200/${INDEX}/${DOC_TYPE}/_mapping" --data-binary "@etc/email_address.mapping"

printf "ingest documents\n"

spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.1.1.jar --conf spark.storage.memoryFraction=.8 spark/elastic_bulk_ingest.py "pst-extract/spark-emailaddr/part-*" "${INDEX}/${DOC_TYPE}"
