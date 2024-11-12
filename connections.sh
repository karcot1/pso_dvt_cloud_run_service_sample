#!/bin/bash

echo "creating DVT connections"

set -e

project_id = ${echo $PROJECT_ID}
terahost=${echo $TERA_HOST}
teraport=${echo $TERA_PORT}
terauser=${echo $TERA_USER}
terapw=${echo $TERRA_PASSWORD}

echo "BigQuery connection"
bqcommand="data-validation connections add --connection-name bq_conn BigQuery --project_id $project_id"
eval $bqcommand

echo "Teradata connection"
tdcommand="data-validation connections add --connection-name td_conn Teradata --host $terahost --port $teraport --user-name $terauser --password $terapw"
eval $tdcommand

exit 0