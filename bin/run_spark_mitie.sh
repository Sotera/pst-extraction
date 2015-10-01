#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-entity" ]]; then
    rm -rf "pst-extract/spark-emails-entity"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files mitie.py,libmitie.so,ner_model.dat spark/mitie_entity_ingest_file.py pst-extract/spark-emails-with-topics pst-extract/spark-emails-entity
