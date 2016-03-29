#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0"

OUTPUT_DIR=spark-emails-with-communities
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/email_community_assign.py pst-extract/spark-emails-text  pst-extract/spark-emailaddr pst-extract/$OUTPUT_DIR

./bin/validate_lfs.sh $OUTPUT_DIR

