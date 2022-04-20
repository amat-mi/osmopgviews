#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import psycopg2
from make_osm_view import RawView

def db_connect(conf):
    connection = psycopg2.connect(**conf)
    cur = connection.cursor()
    return (cur, connection)
    
def make_views(conf):
    connection_options = conf['connect']
    (cur, connection) = db_connect(connection_options)
    options = conf['options']
    print("Creating views in schema:%s from data in schema:%s..." % (options['view_schema'], options['data_schema']))
  
    for viewfile in RawView.list_view_files():
        view = RawView(viewfile, options, db_config='osmosis_pg')
        if view.active:
            # "drop view" won't work on a materialized view
            try:
                print(view.drop())
                cur.execute(view.drop())
            except:
                connection.rollback()
                cur.close()
                connection.close()
                (cur, connection) = db_connect(connection_options)
                cur.execute(view.drop('materialized'))
            status = 'MATERIALIZED' if view.materialized else 'VIEW'
            cur.execute(view.create())
            if options['post_sql']:
                cur.execute(view.translate_sql(options['post_sql']))
        else:
            status = 'skip'
        connection.commit()
        print('%s [%s]' % (view.view_name, status))
    
        
if __name__ == '__main__':
    try:
        dbconf =  {
            'connect' : {
                'host'       : os.environ['DB_HOST'],
                'port'       : os.environ.get('DB_PORT', '5432'),
                'database'   : os.environ['DB_NAME'],
                'user'       : os.environ['DB_USERNAME'],
                'password'   : os.environ['DB_PASSWORD'],
            },
            'options' : {
                'post_sql'       : os.environ.get('POST_SQL', ''),
                'materialized'   : os.environ.get('MATERIALIZED', 0),
                'data_schema'    : os.environ.get('DB_SCHEMA', 'public'),
                'view_schema'    : os.environ.get('DB_VIEW_SCHEMA', 'osm_views'),
            }
        }
        
        make_views(dbconf)

    except KeyError as e:
        print("You need to export DB_* variables before calling this script.")
        print("%s environment variable not found." % str(e))
        quit()
        
    except Exception as e:
        print(str(e))
        
