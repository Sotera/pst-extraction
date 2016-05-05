#!/usr/bin/env bash


set -e
set +x

echo "===========================================$0"

INGEST_ID=$1
CASE_ID=$2
ALTERNATE_ID=$3
LABEL=$4

OUTPUT_DIR=pst-json
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

mkdir "pst-extract/pst-json/"
./src/eml.py pst-extract/emls pst-extract/$OUTPUT_DIR/ -l 100 --ingest_id $INGEST_ID --case_id $CASE_ID --alt_ref_id $ALTERNATE_ID --label $LABEL

./bin/validate_lfs.sh $OUTPUT_DIR

