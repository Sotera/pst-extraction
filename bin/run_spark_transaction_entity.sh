#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0"

OUTPUT_DIR=spark-emails-transaction
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/extract_transaction.py pst-extract/spark-emails-geoip pst-extract/$OUTPUT_DIR

./bin/validate_lfs.sh $OUTPUT_DIR

