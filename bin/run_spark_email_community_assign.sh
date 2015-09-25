#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-with-communities" ]]; then
    rm -rf "pst-extract/spark-emails-with-communities"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/email_community_assign.py pst-extract/spark-emails-text  pst-extract/spark-emailaddr pst-extract/spark-emails-with-communities
