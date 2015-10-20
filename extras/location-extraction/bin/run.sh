#!/usr/bin/env bash

if [[ $# -ne 3 ]]; then
    printf "run.sh <clavin_index_dir> <input_dir> <output_dir>\n"
    exit 1
fi

java -cp .:lib/* clojure.main src/location.clj ${1} ${2} ${3}
