### pst-extraction

place pst files in `pst-extract/pst/`

1. `bin/explode_psts.sh`  - runs readpst to convert pst to mbox
1. `bin/normalize_mbox.sh` - mbox files to json
1. `bin/run_spark_tika.sh` - tika extract text of attachments
1. `bin/run_tika_content_join.sh` - join attachment text with email json
1. `bin/run_spark_content_split.sh` - removes base64 encoded attachment from emails json and puts the json in to a separate directory 
