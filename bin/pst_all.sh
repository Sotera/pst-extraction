#!/usr/bin/env bash

set -e

INDEX_NAME=$1

./bin/explode_psts.sh
./bin/normalize_mbox.sh

source ./bin/_newman_pipeline.sh "$@"