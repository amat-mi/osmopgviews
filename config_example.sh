#! /bin/bash 

# Run this script with source config.sh before executing scripts

export CONF_NAME='TEST'

export OSM_FILE='/media/data/OSM/data_osmosis/milano.osm'
export BAKPATH='/media/data/OSM/old_data_osmosis'
export OUTPATH='/media/data/OSM/osmosis_tab'

export OSMOSIS_CMD='/usr/local/bin/osmosis'

export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="osmosis"
export DB_USERNAME="osmo"
export DB_PASSWORD="osmo"
export DB_SCHEMA="public"
