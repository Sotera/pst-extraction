#!/usr/bin/env bash

# requires - pip install geoip2

# check for geo database
if [[ ! -f "etc/GeoLite2-City.mmdb" ]]; then
    curl http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz | gzip -d - > etc/GeoLite2-City.mmdb
fi
