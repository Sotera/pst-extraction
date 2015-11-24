#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-attach" ]]; then
    rm -rf "pst-extract/spark-emails-attach"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/attachment_join.py pst-extract/post-spam-filter/ pst-extract/spark-attach/ pst-extract/spark-emails-attach
