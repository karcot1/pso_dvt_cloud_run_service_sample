#!/bin/bash

echo "creating DVT connections"

set -e

# terahost=${echo $TERA_HOST}
# teraport=${echo $TERA_PORT}
# terauser=${echo $TERA_USER}
# terapw=${echo $TERRA_PASSWORD}

echo "BigQuery connection"
bqcommand="data-validation connections add --connection-name bq_conn BigQuery --project-id $1"
echo $bqcommand
eval $bqcommand

# echo "Teradata connection"
# tdcommand="data-validation connections add --connection-name td_conn Teradata --host $terahost --port $teraport --user-name $terauser --password $terapw"
# eval $tdcommand

echo "Testing FileSystem src connection"
fscommand="data-validation connections add --connection-name FILE_conn1 FileSystem --table-name file1_table --file-path  gs://dvt_filesystem_conn_test1/test_src.csv --file-type csv"
echo $fscommand
eval $fscommand

echo "Testing FileSystem tgt connection"
fscommand="data-validation connections add --connection-name FILE_conn2 FileSystem --table-name file2_table --file-path  gs://dvt_filesystem_conn_test1/test_tgt.csv --file-type csv"
echo $fscommand
eval $fscommand

exit 0