#!/usr/bin/env bash

set +x
set -e

if [[ -d "pst-extract/mbox/" ]]; then
    rm -rf "pst-extract/mbox/"
fi

mkdir "pst-extract/mbox/"

for f in pst-extract/pst/*.pst;
do
    readpst -r -j 8 -o pst-extract/mbox ${f};
done;
