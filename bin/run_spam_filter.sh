#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/post-spam-filter" ]]; then
    rm -rf "pst-extract/post-spam-filter"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/naive_bayes_classifier.pkl spark/spam_filter_harness.py /vagrant/pst-extraction/pst-extract/pst-json /vagrant/pst-extraction/pst-extract/post-spam-filter
