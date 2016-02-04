#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-text" ]]; then
    rm -rf "pst-extract/spark-emails-text"
fi

if [[ -d "pst-extract/spark-emails-attachments" ]]; then
    rm -rf "pst-extract/spark-emails-attachments"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/attachment_split.py pst-extract/spark-emails-attach-exif pst-extract/spark-emails-text  pst-extract/spark-emails-attachments
