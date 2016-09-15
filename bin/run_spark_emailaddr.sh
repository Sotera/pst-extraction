#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0 $@"

INGEST_ID=$1
CASE_ID=$2
ALTERNATE_ID=$3
LABEL=$4
JSON_VALIDATION_FLAG=$5

OUTPUT_DIR=spark-emailaddr
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/emailaddr_agg.py pst-extract/spark-emails-text pst-extract/$OUTPUT_DIR --ingest_id $INGEST_ID --case_id $CASE_ID --alt_ref_id $ALTERNATE_ID --label $LABEL $JSON_VALIDATION_FLAG

./bin/validate_lfs.sh $OUTPUT_DIR
