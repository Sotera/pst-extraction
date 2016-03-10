#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-geoip" ]]; then
    rm -rf "pst-extract/spark-emails-geoip"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 spark/geoip_extraction.py pst-extract/spark-emails-entity pst-extract/spark-emails-geoip --geodb /etc/GeoLite2-City.mmdb
