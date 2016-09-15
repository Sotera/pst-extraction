#!/usr/bin/env bash

set +x
set -e
echo "===========================================$0"
echo "es_import <index name> <backup.json> conf/env.cfg"
if [[ $# -lt 1 ]]; then
    printf "missing configuration\n"
    exit 1
fi

source $3

#host is never localhost since that is the docker container
ES_HOST=172.17.0.1

ES_INDEX=$1
INDEX_NAME=$1
BACKUP_FILE=$2


printf "create doc_type for <${ES_INDEX}> \n"

response=$(curl -s -XPUT -i --write-out %{http_code} --silent --output /dev/null "${ES_HOST}:${ES_PORT}/${ES_INDEX}" --data-binary "@etc/newman_es_mappings.json")

if [[ "$response" -eq 400 ]]; then
    printf "ERROR:  You must clear the index <${ES_INDEX}> before ingesting data."
    exit 4
fi

if [[ ! "$response" -eq 200 ]]; then
    printf "ERROR:  Error $response while creating index mappings.  Pipeline ingest halted.  Check mappings and elasticsearch logs for further details.\n\n"
    exit 5
fi

printf "Successfully created index mappings for <${ES_INDEX}> \n"

docker run --rm -ti -v ${BACKUP_FILE}:${BACKUP_FILE} taskrabbit/elasticsearch-dump \
  --input=${BACKUP_FILE} \
  --output=http://${ES_HOST}:9200/${INDEX_NAME} \
  --type=data
