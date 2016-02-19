#!/usr/bin/env bash

set +x

if [[ -d "tmp/attachments" ]]; then
    rm -rf  "tmp/attachments"
fi


mkdir -p tmp/attachments

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/dump_attachment-binaries.py pst-extract/pst-json "tmp/attachments"
