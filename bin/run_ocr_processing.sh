#!/usr/bin/env bash

set +x
set -e

echo "===========================================$0"

OUTPUT_DIR='ocr_output'
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --files spark/filters.py,ocr/ocr_opencv.py,ocr/resize_image.py --conf spark.storage.memoryFraction=.8 ocr/run_ocr.py pst-extract/pst-json pst-extract/$OUTPUT_DIR

