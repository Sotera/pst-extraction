#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/example/spark-attach" ]]; then
    rm -rf "pst-extract/example/spark-attach"
fi

spark-submit --master local[*] --driver-memory 8g --jars lib/tika-app-1.10.jar --conf spark.storage.memoryFraction=.8 --class newman.Driver lib/newman-attachment-text-extract_2.10-1.0.0.jar pst-extract/example/pst-json/ pst-extract/example/spark-attach etc/exts.txt 
