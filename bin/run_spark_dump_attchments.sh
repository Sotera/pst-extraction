#!/usr/bin/env bash
#
# Write all the attachments to lfs

set +x

if [[ -d "tmp/attachments" ]]; then
    rm -rf  "tmp/attachments"
fi


mkdir -p tmp/attachments

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/dump_attachments.py pst-extract/pst-json "tmp/attachments"
