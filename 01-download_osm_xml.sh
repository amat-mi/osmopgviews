#! /bin/bash 

# (around[.input_set]:radius)
OVERPASS_QUERY="[timeout:180];area(3600044915); (node(area); <; >); out meta;"

echo "Backup previously downloaded data..."

NOW=$(date +%Y%m%d-%H%M%S)
tar cfz $BAKPATH/data_$NOW.tgz $OSM_FILE
rm -f $OSM_FILE

echo "Download OSM file of Milano..."
wget -q -O- "http://overpass-api.de/api/kill_my_queries"
wget -O $OSM_FILE "http://overpass-api.de/api/interpreter?data=$OVERPASS_QUERY"

