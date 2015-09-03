# pst-extraction

pst extraction


place pst files in ```pst-extract/pst/``` 

1. ```bin/explode_psts.sh``  - runs readpst to convert pst to mbox
1. ```bin/normalize_mbox.sh``` - mbox files to json
1. ```bin/run_spark_tika.sh``` - tika extract text of attachments
1. ```bin/run_tika_content_join.sh``` - join attachment text with email json
