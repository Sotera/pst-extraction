#!/usr/bin/env bash

set +x
set -e

echo "===========================================$0"

OUTPUT_DIR='spark-image-classifier'
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --py-files image-detection/neuro.zip --files image-detection/hog_gist_feature_extraction.py,image-detection/default_param.py,image-detection/gabor_features.py,image-detection/human_classifier.pkl,image-detection/receipt_classifier.pkl,image-detection/SLIP.py,image-detection/resize_image.py,image-detection/LogGabor.py  image-detection/run_human_receipt_detection.py pst-extract/pst-json pst-extract/$OUTPUT_DIR

