#!/usr/bin/env bash


#host is never localhost since that is the docker container
ES_HOST=172.17.0.1
INDEX_NAME=$1
BACKUP_DIR=$2

# Copy an index from production to staging with mappings:
#docker run --rm -ti -v ${BACKUP_DIR}:${BACKUP_DIR} taskrabbit/elasticsearch-dump \
#  --input=http://${ES_HOST}:9200/${INDEX_NAME} \
#  --output=${BACKUP_DIR}/${INDEX_NAME}_mapping.json \
#  --type=mapping
docker run --rm -ti -v ${BACKUP_DIR}:${BACKUP_DIR} taskrabbit/elasticsearch-dump \
  --input=http://${ES_HOST}:9200/${INDEX_NAME} \
  --output=${BACKUP_DIR}/${INDEX_NAME}.json \
  --type=data
#docker run --rm -ti -v ${BACKUP_DIR}/${INDEX_NAME}:${BACKUP_DIR}/${INDEX_NAME} taskrabbit/elasticsearch-dump \
#  --input=http://${ES_HOST}:9200/${INDEX_NAME} \
#  --output=${BACKUP_DIR}/${INDEX_NAME} \
#  --type=data
