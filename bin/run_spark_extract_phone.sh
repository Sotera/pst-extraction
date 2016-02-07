#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-with-phone" ]]; then
    rm -rf "pst-extract/spark-emails-with-phone"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/phone_numbers.py pst-extract/spark-emails-attach pst-extract/spark-emails-with-phone
