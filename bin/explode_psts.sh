#!/usr/bin/env bash

set +x
set -e

for f in pst-extract/pst/*.pst;
do
    readpst -r -j 8 -o pst-extract/mbox ${f};
done;
