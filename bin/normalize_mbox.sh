#!/usr/bin/env bash

if [[ -d "pst-extract/pst-json/" ]]; then
    rm -rf "pst-extract/pst-json/"
fi

mkdir "pst-extract/pst-json/"
./src/mbox.py pst-extract/mbox pst-extract/pst-json/
