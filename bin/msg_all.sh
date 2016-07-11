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
PST_EXTRACT_HOME=/srv/software/pst-extraction/pst-extract/
#MSGS_HOME=/mnt/FERC/somedata/

./bin/normalize_msg.sh "$@"
#docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ -v $PST_EXTRACT_HOME:/srv/software/pst-extraction/pst-extract/ -v $MSGS_HOME:/srv/software/pst-extraction/pst-extract/msgs msg_extractor ./bin/normalize_msg.sh "$@"

./bin/run_spark_tika.sh
docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ -v $NEWMAN_RESEARCH_OCR_HOME/spark-newman-extraction:/srv/software/pst-extraction/ocr ocr ./bin/run_ocr_processing.sh
docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ -v $NEWMAN_RESEARCH_OCR_HOME/spark-newman-human-receipt-detection:/srv/software/pst-extraction/image-detection ocr ./bin/run_human_receipt_detection_harness.sh

./bin/run_binary_extraction_merge.sh

./bin/run_spark_extract_phone.sh
./bin/run_spark_exif_attachments.sh

./bin/run_spark_content_split.sh

./bin/run_spark_emailaddr.sh $INGEST_ID $CASE_ID $ALTERNATE_ID $LABEL
./bin/run_spark_email_community_assign.sh
./bin/run_spark_topic_clustering.sh
docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ apertium ./bin/run_spark_translation.sh $FORCE_LANGUGE
./bin/run_spark_mitie.sh
docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ geo-utils ./bin/run_spark_geoip.sh

./bin/run_spark_transaction_entity.sh

./bin/run_es_ingest.sh $INGEST_ID $CASE_ID $ALTERNATE_ID $LABEL conf/env.cfg

echo "==============================================================Completed newman extraction pipeline====================================================="
