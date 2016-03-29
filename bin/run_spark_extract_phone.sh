#!/usr/bin/env bash

set -e
set +x

echo "===========================================$0"

OUTPUT_DIR=spark-emails-with-phone
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi


spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/phone_numbers.py pst-extract/spark-emails-attach pst-extract/$OUTPUT_DIR

./bin/validate_lfs.sh $OUTPUT_DIR
