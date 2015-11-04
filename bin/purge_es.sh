#!/usr/bin/env bash

set +x
set -e

if [[ $# -lt 1 ]]; then
    printf "missing configuration\n"
    exit 1
fi

source $1

if [[ $# -lt 2 ]]; then
    printf "missing doc_type argument\n"
    exit 1
fi

curl -XDELETE "${ES_HOST}:${ES_PORT}/${ES_INDEX}/$2"
