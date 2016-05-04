#!/usr/bin/env bash

set +x
set -e

if [[ $# -lt 1 ]]; then
    printf "missing configuration\n"
    exit 1
fi

source $1

curl -XDELETE "${ES_HOST}:${ES_PORT}/${ES_INDEX}"
