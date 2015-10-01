#!/usr/bin/env bash

set +x
set -e

INDEX=sample
<<<<<<< HEAD
DOC_TYPE=emails
=======
DOC_TYPE=entity
>>>>>>> ed67238cf2e414cfeaa33d9ee2e0c67e2c908352

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
<<<<<<< HEAD
curl -s -XPUT "http://localhost:9200/${INDEX}/${DOC_TYPE}/_mapping" --data-binary "@etc/emails.mapping"
=======
curl -s -XPUT "http://localhost:9200/${INDEX}/${DOC_TYPE}/_mapping" --data-binary "@etc/entity.mapping"
>>>>>>> ed67238cf2e414cfeaa33d9ee2e0c67e2c908352


printf "ingest entity documents\n"

spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.1.1.jar --conf spark.storage.memoryFraction=.8 spark/elastic_bulk_ingest.py "pst-extract/spark-emails-entity/part-*" "${INDEX}/${DOC_TYPE}"
