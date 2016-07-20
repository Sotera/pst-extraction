#!/usr/bin/env bash

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

#Make sure to exit if anything fails
set -e

CURRENT_DIR=$(pwd)

./bin/run_spark_tika.sh

#Added for spam filter and ocr processing:
#-------------------
#./bin/run_spam_filter.sh
#-------------------

docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ -v $NEWMAN_RESEARCH_OCR_HOME/spark-newman-extraction:/srv/software/pst-extraction/ocr ocr ./bin/run_ocr_processing.sh
docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ -v $NEWMAN_RESEARCH_OCR_HOME/spark-newman-human-receipt-detection:/srv/software/pst-extraction/image-detection ocr ./bin/run_human_receipt_detection_harness.sh


#Merge step which will add the tika content and the image_analytics back to the original doc
./bin/run_binary_extraction_merge.sh

./bin/run_spark_extract_phone.sh
./bin/run_spark_exif_attachments.sh

#Step to remove the base64 binary attachments from the email doc
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
