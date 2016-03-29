#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0"

OUTPUT_DIR=spark-emails-entity
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files mitie.py,libmitie.so,ner_model_english.dat,ner_model_spanish.dat spark/mitie_entity_ingest_file.py pst-extract/spark-emails-translation pst-extract/$OUTPUT_DIR --extract_field body

./bin/validate_lfs.sh $OUTPUT_DIR
