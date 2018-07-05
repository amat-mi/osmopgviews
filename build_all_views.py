#! /usr/bin/python

import os
import psycopg2
from make_osm_view import RawView

def db_connect(conf):
    connection = psycopg2.connect(**conf)
    cur = connection.cursor()
    return (cur, connection)
    
def make_views(conf, view_schema, data_schema):
    post_sql = conf.pop('post_sql', '')
    (cur, connection) = db_connect(conf)
  
    for viewfile in RawView.list_view_files():
        view = RawView(viewfile, db_config='osmosis_pg', view_schema=view_schema, data_schema=data_schema)
        if view.active:
            # "drop view" won't work on a materialized view
            try:
                cur.execute(view.drop())
            except:
                connection.rollback()
                cur.close()
                connection.close()
                (cur, connection) = db_connect(conf)
                cur.execute(view.drop('materialized'))
            status = 'MATERIALIZED' if view.materialized else 'VIEW'
            cur.execute(view.create())
            if post_sql:
                cur.execute(view.translate_sql(post_sql))
        else:
            status = 'skip'
        connection.commit()
        print '%s [%s]' % (view.view_name, status)
    
        
if __name__ == '__main__':
    try:
        dbconf =  {
            'host'       : os.environ['DB_HOST'],
            'port'       : os.environ.get('DB_PORT', '5432'),
            'database'   : os.environ['DB_NAME'],
            'user'       : os.environ['DB_USERNAME'],
            'password'   : os.environ['DB_PASSWORD'],
            'post_sql'   : os.environ.get('POST_SQL', ''),
        }
        
        data_schema = os.environ.get('DB_SCHEMA', 'public')
        view_schema = os.environ.get('DB_VIEW_SCHEMA', 'osm_views')
        print "Creating views in %s schema from data in %s..." % (view_schema, data_schema)
        make_views(dbconf, view_schema, data_schema)

    except KeyError, e:
        print "You need to export DB_* variables before calling this script."
        print "%s environment variable not found." % str(e)
        quit()
        
    except Exception, e:
        print str(e)
        
