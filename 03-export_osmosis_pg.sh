#! /bin/bash 

# Create the views
VIEWS="aree_pedonali crossing highway locref_no_tags multipoly_ways subway subway_entrance subway_station traffic_sign tram pedestrian"
# turn_restrictions 

for VIEW in $VIEWS
do
    echo "Exporting $VIEW..."
    ogr2ogr -skipfailures -t_srs epsg:3003 -f "MapInfo file" $OUTPATH/$VIEW.tab "PG:dbname='osmosis' host='$DB_HOST' port='$DB_PORT' user='$DB_USERNAME' password='$DB_PASSWORD' active_schema=$DB_SCHEMA" $VIEW
done;
