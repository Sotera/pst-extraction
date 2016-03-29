#!/usr/bin/env bash

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
docker run -ti --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ apertium ./bin/run_spark_translation.sh
./bin/run_spark_mitie.sh
docker run -ti --rm -P -v $CURRENT_DIR:/srv/software/pst-extraction/ geo-utils ./bin/run_spark_geoip.sh

./bin/run_es_ingest.sh conf/env.cfg

