#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emailaddr" ]]; then
    rm -rf "pst-extract/spark-emailaddr"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/emailaddr_agg.py pst-extract/spark-emails-text pst-extract/spark-emailaddr
