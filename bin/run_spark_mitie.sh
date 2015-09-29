#!/usr/bin/env bash

set +x

if [[ -d "pst-extract/spark-emails-entity" ]]; then
    rm -rf "pst-extract/spark-emails-entity"
fi

#--py-files not working as expected
#/spark/spark/bin/spark-submit --master local[*] --driver-memory 8g --py-files=/srv/software/MITIE/mitielib --conf spark.storage.memoryFraction=.8 spark/emails_entity.py pst-extract/spark-emails-text  pst-extract/spark-emails-entity

/spark/spark/bin/spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files mitie.py,libmitie.so,ner_model.dat spark/mitie_entity_ingest_file.py pst-extract/spark-emails-text  pst-extract/spark-emails-entity
