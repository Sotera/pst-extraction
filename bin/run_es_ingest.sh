#!/usr/bin/env bash

set +x
set -e

echo "===========================================$0 $@"

if [[ $# -lt 1 ]]; then
    printf "missing configuration\n"
    exit 1
fi

source $5

ES_INDEX=$1
CASE_ID=$2
ALTERNATE_ID=$3
LABEL=$4

printf "Attempting to create doc_type for <${ES_INDEX}> \n"

response=$(curl -s -XPUT -i --write-out %{http_code} --silent --output /dev/null --noproxy localhost, "http://${ES_HOST}:${ES_PORT}/${ES_INDEX}" --data-binary "@etc/newman_es_mappings.json")

#DEBUG with just the mapping line
#response=$(curl -s -XPUT -i --write-out %{http_code} "${ES_HOST}:${ES_PORT}/${ES_INDEX}" --data-binary "@etc/newman_es_mappings.json")
#printf "CREATED MAPPING"
#exit 0
#

if [[ "$response" -eq 400 ]]; then
    printf "ERROR:  You must clear the index <${ES_INDEX}> before ingesting data."
    exit 4
fi

if [[ ! "$response" -eq 200 ]]; then
    printf "ERROR:  Error $response while creating index mappings.  Pipeline ingest halted.  Check mappings and elasticsearch logs for further details.\n\n"
    exit 5
fi

printf "Successfully created index mappings for <${ES_INDEX}> \n"

printf "ES ingest init\n"
./src/init_es_index.py ${ES_INDEX} --es_nodes ${ES_NODES} --ingest_id ${ES_INDEX} --case_id ${CASE_ID} --alt_ref_id ${ALTERNATE_ID} --label ${LABEL}

printf "ES ingest lda clusters\n"
./src/upload_lda_clusters.py ${ES_INDEX} --es_nodes ${ES_NODES}


printf "====================ES ingest documents=========================\n"
printf "ES ingest email addresses\n"
spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.2.0-m1.jar --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/elastic_bulk_ingest.py "pst-extract/spark-emailaddr/part-*" "${ES_INDEX}/${ES_DOC_TYPE_EMAILADDR}"  --es_nodes ${ES_NODES}
printf "ES ingest attachments\n"
spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.2.0-m1.jar --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/elastic_bulk_ingest.py "pst-extract/spark-emails-attachments/part-*" "${ES_INDEX}/${ES_DOC_TYPE_ATTACHMENTS}" --id_field guid  --es_nodes ${ES_NODES}
printf "ES ingest emails\n"
spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.2.0-m1.jar --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/elastic_bulk_ingest.py "pst-extract/spark-emails-transaction/part-*" "${ES_INDEX}/${ES_DOC_TYPE_EMAILS}" --id_field id  --es_nodes ${ES_NODES}

