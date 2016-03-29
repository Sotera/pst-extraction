#!/usr/bin/env bash

set -e

./bin/explode_psts.sh
./bin/normalize_mbox.sh

source ./bin/_newman_pipeline.sh