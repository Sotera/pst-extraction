#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-translation" ]]; then
    rm -rf "pst-extract/spark-emails-translation"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/language.py pst-extract/spark-emails-with-topics pst-extract/spark-emails-translation
