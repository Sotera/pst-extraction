#!/usr/bin/env bash

if [[ ! -d "etc/location/" ]]; then
    mkdir -p "etc/location/"
fi
# check for geonames data
if [[ ! -f "etc/allCountries.txt" ]]; then
    #curl http://download.geonames.org/export/dump/allCountries.zip | tar -xf- -C etc/
    #JReeme sez: They changed the format of the allCountries.zip file so tar can't handle it anymore so ...
    wget http://download.geonames.org/export/dump/allCountries.zip -O tmp.zip && unzip tmp.zip -d ./etc && rm tmp.zip

fi

java -cp extras/location-extraction/lib/clavin-2.0.0-jar-with-dependencies.jar -mx4g "com.bericotech.clavin.index.IndexDirectoryBuilder" --gazetteer-files "etc/allCountries.txt" --index-path "etc/location/index"
