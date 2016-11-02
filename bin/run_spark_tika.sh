#!/usr/bin/env bash

set -e
set +x

echo "===========================================$0"

OUTPUT_DIR=spark-attach
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --class com.soteradefense.newman.TikaExtraction lib/newman-spark-tika-0.1-SNAPSHOT-jar-with-dependencies.jar -i pst-extract/pst-json -o pst-extract/$OUTPUT_DIR

./bin/validate_lfs.sh $OUTPUT_DIR