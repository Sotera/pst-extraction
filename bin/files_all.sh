#!/usr/bin/env bash
./bin/normalize_files.sh

./bin/run_spark_tika.sh
./bin/run_tika_content_join.sh

./bin/run_spark_extract_phone.sh
./bin/run_spark_exif_attachments.sh

./bin/run_spark_content_split.sh

./bin/run_spark_mitie_attachments.sh

#Hack step
./bin/run_spark_emailaddr.sh
./bin/run_spark_email_community_assign.sh
#Hack step
./bin/run_spark_es_ingest_emails_file_mode.sh conf/env.cfg
#Hack step
./bin/run_spark_es_ingest_emailaddr.sh conf/env.cfg

./bin/run_spark_es_ingest_document_attachments.sh conf/env.cfg
