#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0"

OUTPUT_DIR=spark-emails-geoip
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/geoip_extraction.py pst-extract/spark-emails-entity pst-extract/$OUTPUT_DIR --geodb /etc/GeoLite2-City.mmdb

./bin/validate_lfs.sh $OUTPUT_DIR

