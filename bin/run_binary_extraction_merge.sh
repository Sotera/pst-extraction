#!/usr/bin/env bash

set -e
set +x

echo "===========================================$0"

INPUT_RIGHT_SIDE_DIRS=pst-extract/spark-attach/
#,pst-extract/ocr_output/
OUTPUT_DIR=spark-emails-attach-text
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

OUTPUT_DIR2=spark-emails-attach
if [[ -d "pst-extract/$OUTPUT_DIR2" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR2"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/attachment_join.py pst-extract/pst-json/ $INPUT_RIGHT_SIDE_DIRS pst-extract/$OUTPUT_DIR
./bin/validate_lfs.sh $OUTPUT_DIR

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/attachment_join.py pst-extract/$OUTPUT_DIR pst-extract/ocr_output/ pst-extract/$OUTPUT_DIR2
./bin/validate_lfs.sh $OUTPUT_DIR2


