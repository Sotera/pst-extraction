#!/usr/bin/env bash

INDEX_NAME=$1

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
./bin/run_tika_content_join.sh

./bin/run_spark_extract_phone.sh
./bin/run_spark_exif_attachments.sh

./bin/run_spark_content_split.sh
./bin/run_spark_emailaddr.sh
./bin/run_spark_email_community_assign.sh
./bin/run_spark_topic_clustering.sh
docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ apertium ./bin/run_spark_translation.sh
./bin/run_spark_mitie.sh
docker run $DOCKER_RUN_MODE --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ geo-utils ./bin/run_spark_geoip.sh

./bin/run_es_ingest.sh $INDEX_NAME conf/env.cfg

echo "==============================================================Completed newman extraction pipeline====================================================="
