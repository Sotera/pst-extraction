#!/usr/bin/env bash

set +x
set -e

INDEX=sample
DOC_TYPE=emails

curl -XDELETE 'http://localhost:9200/${INDEX}'
