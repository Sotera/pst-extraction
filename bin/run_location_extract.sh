#!/usr/bin/env bash

set +x

if [[ ! -d "etc/location/index" ]]; then
    printf "missing clavin index @ etc/location/index\n"
    exit 1
fi

if [[ ! -d "pst-extract/spark-emails-text" ]]; then
    printf "missing input directory pst-extract/spark-emails-text"
    exit 1
fi

if [[ -d "pst-extract/email-locations" ]]; then
    rm -rf "pst-extract/email-locations"
fi

mkdir -p "pst-extract/email-locations"

java -cp .:extras/location-extraction/lib/* clojure.main extras/location-extraction/src/location.clj "etc/location/index" "pst-extract/spark-emails-text" "pst-extract/email-locations"
