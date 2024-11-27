#!/bin/bash

echo "running dvt yamls"

config_file_dir=$1


echo "printing config location"

echo $config_file_dir

command="data-validation configs run -kc -cdir $1"

echo $command
eval $command

echo "script run complete"