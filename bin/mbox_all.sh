#!/usr/bin/env bash

set -e

./bin/normalize_mbox.sh "$@"

source ./bin/_newman_pipeline.sh "$@"