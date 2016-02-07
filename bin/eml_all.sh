#!/usr/bin/env bash

./bin/normalize_eml.sh
./bin/run_spark_tika.sh
./bin/run_tika_content_join.sh

./bin/run_spark_extract_phone.sh
./bin/run_spark_exif_attachments.sh

./bin/run_spark_content_split.sh
./bin/run_spark_emailaddr.sh
./bin/run_spark_email_community_assign.sh
./bin/run_spark_topic_clustering.sh
./bin/run_spark_mitie.sh
./bin/run_spark_es_ingest_emailaddr.sh conf/env.cfg
./bin/run_spark_es_ingest_attachments.sh conf/env.cfg
./bin/run_spark_es_ingest_emails.sh conf/env.cfg
