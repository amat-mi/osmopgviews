# osmopgviews

## Overview

Quickly define and build views on an osmosis postgis database, based on simple tag-based definition stored in .ini files.

## Dependencies

You will need osmosis and a most likely a linux box.
You also need ogr/gdal if you need to export the views in other formats.

## Quick start

First, create a database suitable for osmosis:

    createdb osmosis;
    # Based on which template you use you may need to issue a 
    # create extension postgis;
    # on the newly created db
    /path/to/osmosis/init_osmosis.sh

* Create a config file based on config_example.sh, say "config.sh".
* Edit the 01-download_osm_xml.sh script and adapt the overpass query to your needs.
* Edit the 03-export_osmosis_pg.sh if you don't need to export the views or you need them exported in a format other than MapInfo.
* Change or add views definitions in the views subfolder.

Run the scripts:

    . config.sh
    01-download_osm_xml.sh
    02-load_osm_xml_to_pg.sh
    03-export_osmosis_pg.sh

