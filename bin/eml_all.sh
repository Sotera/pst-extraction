#!/usr/bin/env bash

echo "===========================================$0 $@"

#Make sure to exit if anything fails
set -e
set -x

./bin/normalize_eml.sh "$@"

source ./bin/_newman_pipeline.sh "$@"