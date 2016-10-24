#!/usr/bin/env bash

set +x
set -e

if [[ -d "pst-extract/mbox/" ]]; then
    rm -rf "pst-extract/mbox/"
fi

mkdir "pst-extract/mbox/"

for pst in $(find pst-extract/pst/ -name "*.pst")
do
    mbox_dir="pst-extract/mbox/${pst#pst-extract/pst/}"
    mkdir -p ${mbox_dir}
    echo  "Proccessing pst in path:  $pst => mbox path:  $mbox_dir/"
    readpst -r -j 8 -o ${mbox_dir} ${pst};
done
