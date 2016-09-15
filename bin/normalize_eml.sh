#!/usr/bin/env bash


set -e
set +x

echo "===========================================$0 $@"

INGEST_ID=$1
CASE_ID=$2
ALTERNATE_ID=$3
LABEL=$4

#This is the suffix for the eml files which contain the MIME encoded data
# e.g. for MS hotmail account is usually _mime.txt
SUFFIX=_mime.txt

OUTPUT_DIR=pst-json
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

mkdir "pst-extract/pst-json/"
./src/eml.py pst-extract/emls pst-extract/$OUTPUT_DIR/ -l 100 --ingest_id $INGEST_ID --case_id $CASE_ID --alt_ref_id $ALTERNATE_ID --label $LABEL --suffix $SUFFIX

./bin/validate_lfs.sh $OUTPUT_DIR

