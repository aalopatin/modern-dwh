#!/usr/bin/env bash

KAFKA_HOME=/opt/kafka
CONFIG="$KAFKA_HOME/config/connect-distributed.properties"

update_connect_configs() {
  env | grep '^CONNECT_' | while IFS='=' read -r KEY VALUE; do
	prop_name=$(echo "${KEY#CONNECT_}" | tr '[:upper:]' '[:lower:]' | tr '_' '.')
	
	if grep -qE "^#?$prop_name=" "$CONFIG"; then	 
	  sed -i "s|^#\?$prop_name=.*|$prop_name=$VALUE|" "$CONFIG"
	else
	  echo "$prop_name=$VALUE" >> "$CONFIG"
    fi
  done 
}

update_connect_configs

$KAFKA_HOME/bin/connect-distributed.sh $CONFIG