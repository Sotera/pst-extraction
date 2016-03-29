#!/usr/bin/env bash

set -e
set +x

echo "===========================================$0"

OUTPUT_DIR=pst-json
if [[ -d "pst-extract/$OUTPUT_DIR" ]]; then
    rm -rf "pst-extract/$OUTPUT_DIR"
fi

mkdir "pst-extract/pst-json/"
./src/eml.py pst-extract/emls pst-extract/$OUTPUT_DIR/ -l 100

./bin/validate_lfs.sh $OUTPUT_DIR

