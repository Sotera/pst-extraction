#!/usr/bin/env bash

./bin/run_spark_es_ingest_emailaddr.sh conf/env.cfg
./bin/run_spark_es_ingest_attachments.sh conf/env.cfg
./bin/run_spark_es_ingest_emails.sh conf/env.cfg
