#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-text-org-location" ]]; then
    rm -rf "pst-extract/spark-emails-text-org-location"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files etc/GeoLite2-City.mmdb extras/location-extraction-ip/geoip_extraction.py pst-extract/spark-emails-text pst-extract/spark-emails-text-org-location
