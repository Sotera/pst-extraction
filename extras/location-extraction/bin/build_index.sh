#!/usr/bin/env bash


if [[ $# -ne 1 ]]; then
    printf "build_index.sh <clavin_index_dir>\n"
    exit 1
fi

# check for geonames data 
if [[ ! -f "etc/allCountries.txt" ]]; then
    curl http://download.geonames.org/export/dump/allCountries.zip | tar -xf- -C etc/    
fi

java -cp lib/clavin-2.0.0-jar-with-dependencies.jar -mx4g "com.bericotech.clavin.index.IndexDirectoryBuilder" --gazetteer-files etc/allCountries.txt --index-path ${1}
