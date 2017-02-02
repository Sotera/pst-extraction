#!/usr/bin/env bash

#Make sure to exit if anything fails
set -e

INGEST_ID=$1
CASE_ID=$2
ALTERNATE_ID=$3
LABEL=$4
FORCE_LANGUGE=$5

NEWMAN_RESEARCH_OCR_HOME=/srv/software/newman-research-master/docker_opencv_spark_ocr

#Autodetect terminal setting which is needed to launch docker as a scripted subprocess from tangelo
DOCKER_RUN_MODE=-it

if [ -t 1 ] ; then
  echo running in tty;
else
  echo running in tty;
  DOCKER_RUN_MODE=-i
fi


CURRENT_DIR=$(pwd)

./bin/normalize_columnar_files.sh "$@"

if [[ -d "pst-extract/spark-emails-with-numbers" ]]; then
    rm -rf "pst-extract/spark-emails-with-numbers"
fi
spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/numbers_extractor.py pst-extract/pst-json pst-extract/spark-emails-with-numbers --validate_json

if [[ -d "pst-extract/spark-emails-transaction" ]]; then
    rm -rf "pst-extract/spark-emails-transaction"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/extract_transaction.py pst-extract/spark-emails-with-numbers pst-extract/spark-emails-transaction --validate_json

if [[ -d "pst-extract/spark-emails-entity" ]]; then
    rm -rf "pst-extract/spark-emails-entity"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py,mitie.py,libmitie.so,ner_model_english.dat,ner_model_spanish.dat spark/mitie_entity_ingest_file.py pst-extract/spark-emails-transaction pst-extract/spark-emails-entity --extract_field body ${RUN_FLAGS}


docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ geo-utils ./bin/run_spark_geoip.sh --validate_json


if [[ -d "pst-extract/spark-emailaddr" ]]; then
    rm -rf "pst-extract/spark-emailaddr"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/emailaddr_agg.py pst-extract/pst-json pst-extract/spark-emailaddr --ingest_id $INGEST_ID --case_id $CASE_ID --alt_ref_id $ALTERNATE_ID --label $LABEL --validate_json


echo "======================ES INGEST=====================$0 $@"

if [[ $# -lt 1 ]]; then
    printf "missing configuration\n"
    exit 1
fi

source conf/env.cfg

ES_INDEX=$INGEST_ID
VALIDATE_JSON="--validate_json"

printf "Attempting to create doc_type for <${ES_INDEX}> \n"

response=$(curl -s -XPUT -i --write-out %{http_code} --silent --output /dev/null --noproxy localhost, "http://${ES_HOST}:${ES_PORT}/${ES_INDEX}" --data-binary "@etc/newman_es_mappings_dynamic.json")

#DEBUG with just the mapping line
#response=$(curl -s -XPUT -i --write-out %{http_code} "${ES_HOST}:${ES_PORT}/${ES_INDEX}" --data-binary "@etc/newman_es_mappings.json")
#printf "CREATED MAPPING"

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
spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.4.0.jar --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/elastic_bulk_ingest.py "pst-extract/spark-emailaddr/part-*" "${ES_INDEX}/${ES_DOC_TYPE_EMAILADDR}" --es_nodes ${ES_NODES} --validate_json

printf "ES ingest emails\n"
spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.4.0.jar --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/elastic_bulk_ingest.py "pst-extract/spark-emails-geoip/part-*" "${ES_INDEX}/${ES_DOC_TYPE_EMAILS}" --id_field id --es_nodes ${ES_NODES} --validate_json



echo "==============================================================Completed newman extraction pipeline====================================================="
