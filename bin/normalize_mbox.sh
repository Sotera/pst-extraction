#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0"

INGEST_ID=$1
CASE_ID=$2
ALTERNATE_ID=$3
LABEL=$4

if [[ -d "pst-extract/pst-json/" ]]; then
    rm -rf "pst-extract/pst-json/"
fi

mkdir "pst-extract/pst-json/"
./src/mbox.py pst-extract/mbox pst-extract/pst-json/ --ingest_id $INGEST_ID --case_id $CASE_ID --alt_ref_id $ALTERNATE_ID --label $LABEL
