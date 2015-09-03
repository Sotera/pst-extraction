#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-attach" ]]; then
    rm -rf "pst-extract/spark-attach"
fi

spark-submit --master local[*] --driver-memory 8g --jars lib/tika-app-1.10.jar,lib/commons-codec-1.10.jar --conf spark.storage.memoryFraction=.8 --class newman.Driver lib/tika-extract_2.10-1.0.0.jar pst-extract/pst-json/ pst-extract/spark-attach etc/exts.txt 
