#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0"

INGEST_ID=$1
CASE_ID=$2
ALTERNATE_ID=$3
LABEL=$4
FORCE_LANGUGE=$5

# elastic search
ES_INDEX=${INGEST_ID}
ES_HOST=localhost
ES_PORT=9200
ES_NODES=localhost

# ES doc types
ES_DOC_TYPE_ATTACHMENTS=attachments
ES_DOC_TYPE_EMAILADDR=email_address
ES_DOC_TYPE_EMAILS=emails
ES_DOC_TYPE_CLUSTERING=lda-clustering


if [[ -d "pst-extract/pst-json/" ]]; then
    rm -rf "pst-extract/pst-json/"
fi

mkdir "pst-extract/pst-json/"
./src/xl_reader.py pst-extract/xl pst-extract/pst-json/ --ingest_id $INGEST_ID --case_id $CASE_ID --alt_ref_id $ALTERNATE_ID --label $LABEL


NEWMAN_RESEARCH_OCR_HOME=/srv/software/newman-research-master/docker_opencv_spark_ocr

#Autodetect terminal setting which is needed to launch docker as a scripted subprocess from tangelo
DOCKER_RUN_MODE=-it

if [ -t 1 ] ; then
  echo running in tty;
else
  echo running in tty;
  DOCKER_RUN_MODE=-i
fi

#Make sure to exit if anything fails
set -e

CURRENT_DIR=$(pwd)

#./bin/run_spark_tika.sh
#
##Added for spam filter and ocr processing:
##-------------------
##./bin/run_spam_filter.sh
##-------------------
#
#docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ -v $NEWMAN_RESEARCH_OCR_HOME/spark-newman-extraction:/srv/software/pst-extraction/ocr ocr ./bin/run_ocr_processing.sh
#docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ -v $NEWMAN_RESEARCH_OCR_HOME/spark-newman-human-receipt-detection:/srv/software/pst-extraction/image-detection ocr ./bin/run_human_receipt_detection_harness.sh
#
#
##Merge step which will add the tika content and the image_analytics back to the original doc
#./bin/run_binary_extraction_merge.sh

#./bin/run_spark_extract_numbers.sh
#./bin/run_spark_exif_attachments.sh
#
##Step to remove the base64 binary attachments from the email doc
#./bin/run_spark_content_split.sh

#bin/run_spark_emailaddr.sh $INGEST_ID $CASE_ID $ALTERNATE_ID $LABEL

if [[ -d "pst-extract/spark-emailaddr" ]]; then
    rm -rf "pst-extract/spark-emailaddr"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/emailaddr_agg.py pst-extract/pst-json pst-extract/spark-emailaddr --ingest_id $INGEST_ID --case_id $CASE_ID --alt_ref_id $ALTERNATE_ID --label $LABEL


#./bin/run_spark_email_community_assign.sh

#HACK
if [[ -d "pst-extract/spark-emails-with-communities" ]]; then
    rm -rf "pst-extract/spark-emails-with-communities"
fi
ln -s pst-json pst-extract/spark-emails-with-communities
./bin/run_spark_topic_clustering.sh

#docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ apertium ./bin/run_spark_translation.sh $FORCE_LANGUGE
#./bin/run_spark_mitie.sh

OUTPUT_DIR=spark-emails-transaction
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi
spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py,mitie.py,libmitie.so,ner_model_english.dat,ner_model_spanish.dat spark/mitie_entity_ingest_file.py pst-extract/spark-emails-with-topics pst-extract/$OUTPUT_DIR --extract_field body

#docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ geo-utils ./bin/run_spark_geoip.sh

#./bin/run_spark_transaction_entity.sh

#ES INGEST
printf "Attempting to create doc_type for <${ES_INDEX}> \n"

response=$(curl -s -XPUT -i --write-out %{http_code} --silent --output /dev/null "${ES_HOST}:${ES_PORT}/${ES_INDEX}" --data-binary "@etc/newman_es_mappings.json")

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
printf "ES ingest emails\n"
spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.4.0.jar --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/elastic_bulk_ingest.py "pst-extract/spark-emails-transaction/part-*" "${ES_INDEX}/${ES_DOC_TYPE_EMAILS}" --id_field id  --es_nodes ${ES_NODES}

printf "ES ingest email addresses\n"
spark-submit --master local[*] --driver-memory 8g --jars lib/elasticsearch-hadoop-2.4.0.jar --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/elastic_bulk_ingest.py "pst-extract/spark-emailaddr/part-*" "${ES_INDEX}/${ES_DOC_TYPE_EMAILADDR}"  --es_nodes ${ES_NODES}


echo "==============================================================Completed newman extraction pipeline====================================================="
