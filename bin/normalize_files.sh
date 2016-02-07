#!/usr/bin/env bash

if [[ -d "pst-extract/pst-json/" ]]; then
    rm -rf "pst-extract/pst-json/"
fi

mkdir "pst-extract/pst-json/"
./src/filecrawl.py pst-extract/files pst-extract/pst-json/ -l 100
