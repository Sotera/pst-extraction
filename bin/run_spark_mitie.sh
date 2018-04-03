#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0 $@"

RUN_FLAGS=$@
echo "mode=$RUN_FLAGS"

OUTPUT_DIR=spark-emails-entity
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py,mitie.py,libmitie.so,ner_model_english.dat,ner_model_spanish.dat spark/spacy_entity_ingest_file.py pst-extract/spark-emails-translation pst-extract/$OUTPUT_DIR --extract_field body ${RUN_FLAGS}
#spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files spark/filters.py,mitie.py,libmitie.so,ner_model_english.dat,ner_model_spanish.dat spark/mitie_entity_ingest_file.py pst-extract/spark-emails-translation pst-extract/$OUTPUT_DIR --extract_field body ${RUN_FLAGS}

./bin/validate_lfs.sh $OUTPUT_DIR
