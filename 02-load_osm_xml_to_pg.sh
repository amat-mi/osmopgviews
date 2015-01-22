#! /bin/bash


echo "*** Truncating database"

$OSMOSIS_CMD --truncate-pgsql host="$DB_HOST:$DB_PORT" database="$DB_NAME" user="$DB_USERNAME" password="$DB_PASSWORD"

echo "*** Replacing file content into database"

$OSMOSIS_CMD --read-xml file=$OSM_FILE --write-pgsql host="$DB_HOST:$DB_PORT" database="$DB_NAME" user="$DB_USERNAME" password="$DB_PASSWORD"
