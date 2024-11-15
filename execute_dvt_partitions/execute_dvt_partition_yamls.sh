#!/bin/bash

echo "running dvt yamls"

config_file_dir=$1
project_id=$2
source_type=$3
target_type=$4


echo "printing"

echo $config_file_dir
echo $project_id
echo $source_type
echo $target_type


# tera_host=$(echo $TERA_HOST)
# tera_port=$(echo $TERA_PORT)
# tera_user=$(echo $TERA_USERNAME)
# tera_password=$(echo $TERA_PASSWORD)

yaml_file_label=$(echo $YAML_FILE_LABEL)


echo "BigQuery connection"
bqcommand="data-validation connections add --connection-name bq_conn BigQuery --project-id $2"
echo $bqcommand
eval $bqcommand

# echo "Teradata connection"
# tdcommand="data-validation connections add --connection-name td_conn Teradata --host $tera_host --port $tera_port --user-name $tera_user --password $tera_password"
# eval $tdcommand


command="data-validation configs run -kc -cdir $1"

echo $command
eval $command

echo "script run complete"