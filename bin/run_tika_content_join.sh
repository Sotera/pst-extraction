#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-content" ]]; then
    rm -rf "pst-extract/spark-emails-content"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/attachment_join.py pst-extract/pst-json/ pst-extract/spark-attach/ pst-extract/spark-emails-content
