#!/usr/bin/env bash

set -e
set +x

echo "===========================================$0 $@"
#
#Mode should be either left off or --docex_mode which will enable copy of extracted data into the body field
#
RUN_FLAGS=$@
echo "mode=$RUN_FLAGS"

INPUT_RIGHT_SIDE_DIRS=pst-extract/spark-attach/
#,pst-extract/ocr_output/
OUTPUT_DIR=spark-emails-attach-text
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi
OUTPUT_DIR2=spark-emails-attach-classification
if [[ -d "pst-extract/$OUTPUT_DIR2" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR2"
fi

OUTPUT_DIR_FINAL=spark-emails-attach
if [[ -d "pst-extract/$OUTPUT_DIR_FINAL" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR_FINAL"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/attachment_join.py pst-extract/pst-json/ $INPUT_RIGHT_SIDE_DIRS pst-extract/$OUTPUT_DIR $RUN_FLAGS
./bin/validate_lfs.sh $OUTPUT_DIR

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/attachment_join.py pst-extract/$OUTPUT_DIR pst-extract/spark-image-classifier/ pst-extract/$OUTPUT_DIR2 $RUN_FLAGS
./bin/validate_lfs.sh $OUTPUT_DIR2

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/attachment_join.py pst-extract/$OUTPUT_DIR2 pst-extract/ocr_output/ pst-extract/$OUTPUT_DIR_FINAL $RUN_FLAGS
./bin/validate_lfs.sh $OUTPUT_DIR_FINAL


