#!/usr/bin/env bash
#TODO temporary until we can unify doc and email attachment pipelines
#TODO Then this will be removed

set +x

if [[ -d "pst-extract/spark-attachments_entities" ]]; then
    rm -rf "pst-extract/spark-attachments_entities"
fi

spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files mitie.py,libmitie.so,ner_model.dat spark/spacy_entity_ingest_file.py pst-extract/spark-emails-attachments pst-extract/spark-attachments_entities --extract_field content

#spark-submit --master local[*] --driver-memory 8g --conf spark.storage.memoryFraction=.8 --files mitie.py,libmitie.so,ner_model.dat spark/mitie_entity_ingest_file.py pst-extract/spark-emails-attachments pst-extract/spark-attachments_entities --extract_field content
