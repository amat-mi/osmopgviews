#! /bin/bash 


# Create the views
VIEWS=`./build_all_views.py | grep -v 'skip' | grep '\[' | awk '{print $1}'`

for VIEW in $VIEWS
do
    echo "Exporting $VIEW..."
    ogr2ogr -t_srs epsg:3003 -f "MapInfo file" $OUTPATH/$VIEW.tab "PG:dbname='osmosis' host='$DB_HOST' port='$DB_PORT' user='$DB_USERNAME' password='$DB_PASSWORD' active_schema=$DB_SCHEMA" $VIEW
done;
