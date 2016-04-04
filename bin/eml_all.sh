#!/usr/bin/env bash

#Make sure to exit if anything fails
set -e

./bin/normalize_eml.sh

source ./bin/_newman_pipeline.sh "$@"