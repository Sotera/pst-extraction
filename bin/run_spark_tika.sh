#!/usr/bin/env bash

set -e
set +x

echo "===========================================$0"

OUTPUT_DIR=spark-attach
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --jars lib/tika-app-1.10.jar,lib/commons-codec-1.10.jar --conf spark.storage.memoryFraction=.8 --class newman.Driver lib/tika-extract_2.10-1.0.1.jar pst-extract/pst-json/ pst-extract/$OUTPUT_DIR etc/exts.txt

./bin/validate_lfs.sh $OUTPUT_DIR
