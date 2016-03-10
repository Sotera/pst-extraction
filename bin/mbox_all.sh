#!/usr/bin/env bash

CURRENT_DIR=$(pwd)

./bin/normalize_mbox.sh
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
./bin/run_spark_es_ingest_emailaddr.sh conf/env.cfg
./bin/run_spark_es_ingest_attachments.sh conf/env.cfg
./bin/run_spark_es_ingest_emails.sh conf/env.cfg
