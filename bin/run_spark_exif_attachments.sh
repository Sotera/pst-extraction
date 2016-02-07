#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-attach-exif" ]]; then
    rm -rf "pst-extract/spark-emails-attach-exif"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/image_exif_processing.py pst-extract/spark-emails-with-phone pst-extract/spark-emails-attach-exif
