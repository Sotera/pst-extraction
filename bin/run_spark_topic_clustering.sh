#!/usr/bin/env bash


RUN_FLAGS=$@
echo "mode=$RUN_FLAGS"

set +x
set -e
echo "===========================================$0"

if [[ -d "pst-extract/spark-lda-input" ]]; then
    rm -rf "pst-extract/spark-lda-input"
fi

if [[ -f "tmp/vocab.idx" ]]; then
    rm -rf "tmp/vocab.idx"
fi

if [[ -f "tmp/lda.map.txt" ]]; then
    rm -rf "tmp/lda.map.txt"
fi

if [[ -d "pst-extract/spark-lda-results" ]]; then
    rm -rf  "pst-extract/spark-lda-results"
fi

OUTPUT_DIR=spark-emails-with-topics
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/topic_clustering.py --stopwords etc/english.stopwords --vocab_index tmp/vocab.idx pst-extract/spark-emails-with-communities pst-extract/spark-lda-input ${RUN_FLAGS}

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --class newman.Driver lib/newman-lda-topics_2.10-1.0.0.jar "pst-extract/spark-lda-input/part-*" "pst-extract/spark-lda-results" "tmp/vocab.idx" "tmp/lda.map.txt"

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py spark/lda_merge.py pst-extract/spark-emails-with-communities pst-extract/spark-lda-results pst-extract/$OUTPUT_DIR ${RUN_FLAGS}

./bin/validate_lfs.sh $OUTPUT_DIR
