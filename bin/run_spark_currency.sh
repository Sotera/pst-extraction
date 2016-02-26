#!/usr/bin/env bash
#
# Write all the attachments to lfs

set +x

mkdir -p tmp

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/extract_transaction.py pst-extract/spark-emails-text "tmp/currency.csv"
