#!/usr/bin/env bash

set +x
set -e

if [[ -d "pst-extract/emls/" ]]; then
    rm -rf "pst-extract/emls/"
fi

mkdir "pst-extract/emls/"

for f in pst-extract/pst/*.pst;
do
    readpst -r -j 8 -e -o pst-extract/emls ${f};
done;
