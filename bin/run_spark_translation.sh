#!/usr/bin/env bash

FORCE_LANGUGE=$1
VALIDATE_JSON=$2
JOSHUA_SERVER="10.1.70.200:8001"

set +x
set -e
echo "===========================================$0 $@"

OUTPUT_DIR=spark-emails-translation
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --files spark/moses_translator.py,spark/filters.py --conf spark.storage.memoryFraction=.8 spark/translation.py pst-extract/spark-emails-with-topics pst-extract/$OUTPUT_DIR --force_language $FORCE_LANGUGE ${VALIDATE_JSON} --translation_mode joshua --moses_server localhost:8080 --joshua_server $JOSHUA_SERVER

./bin/validate_lfs.sh $OUTPUT_DIR
