#!/usr/bin/env bash

if [[ -d "pst-extract/pst-json/" ]]; then
    rm -rf "pst-extract/pst-json/"
fi

mkdir "pst-extract/pst-json/"
./src/eml.py pst-extract/emls pst-extract/pst-json/ -l 100
