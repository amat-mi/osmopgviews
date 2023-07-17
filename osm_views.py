#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usare localsettings.py per condifurare le seguenti variabili:

# Se non definita, cerca nella cartella views del progetto  
VIEWS_INI_FOLDER='/mnt/N/CollaborazioneDiretta/sis/osm_views'
DEFAULT_SCHEMA='osm_views'

La configurazione DB va lanciata con

. dbconnect
Selezionando osmosis come database

"""

import os.path
import glob
import psycopg2
import fnmatch
import argparse


"""

# TODO: introdurre la possibilità di aggiungere i tag che provengono dalle relazioni
[RELATION1]
rel_name=<fied_prefix>
rel_type=
rel_role=
rel_tags=tags restituiti: <prefix>__<tag>

# Rebuild all
./osm_views

# Rebuild footway views
./osm_views 'footway*'

"""

DB_CONF_SPATIALITE = {
        'node' : {
            'geom_table' : 'osm_nodes',
            'tags_table' : 'osm_node_tags',
            'tags_fk'    : 'node_id',
            'geom_field' : 'geom',
            'use_hstore' : False,
        },
        'way' : {
            'geom_table' : 'osm_ways',
            'tags_table' : 'osm_way_tags',
            'tags_fk'    : 'way_id',
            'geom_field' : 'geom',
            'use_hstore' : False,
        },
}

DB_CONF_OSMOSIS = {
        'node' : {
            'geom_table' : 'nodes',
            'geom_field' : 'geom',
            'use_hstore' : True,
            'tags_table' : 'node_tags',
            'tags_fk'    : 'id',
        },
        'way' : {
            'geom_table' : 'ways',
            'geom_field' : 'linestring',
            'use_hstore' : True,
            'tags_table' : 'way_tags',
            'tags_fk'    : 'id',
        },
}


VIEWS_INI_FOLDER=''
DEFAULT_SCHEMA=''

try:
    from localsettings import *
except ImportError:
    pass



class App(object):
    """
    Application 
    """
    COMMANDS = ('list', 'info', 'drop', 'create', 'sql')

    HELP_EPILOG = """
# ESEMPI DI UTILIZZO

# Elenca le viste definite
./osm_views.py list

# Mostra i dettagli sulle viste definite il cui nome comincia con highway
./osm_views.py info --views 'highway*'

# Elimina tutte le viste configurate, materializzate e non, nello schema specificato
./osm_views.py drop --schema=osm_views --view='*'

# (Ri)crea le viste configurate il cui nome comincia con highway, nello schema specificato
./osm_views.py create --schema=osm_views --view='highway*'

# Come sopra, ma crea le viste materializzate
./osm_views.py create --schema=osm_views_mat --materialized --view='highway*'

"""

    
    def __init__(self):
        pass


    def run(self):
#        try:
            self.parse_args()
            self.get_views()
            self.execute_command()
#        except Exception as e:
#            print(str(e))
#            exit(1)    
    
        
    def parse_args(self):
        parser = argparse.ArgumentParser(
            description='Gestisce le viste sui dati OSM caricati da osmosis'
        )
        parser.epilog = App.HELP_EPILOG
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        
        parser.add_argument('command', choices=App.COMMANDS,
            help="Comando da eseguire")
        
        parser.add_argument('--spatialite', action='store_true',
            dest='spatialite', help="Utilizza un DB spatialite anziché PostGIS"
        )
        parser.add_argument('--materialized', action='store_true',
            dest='materialized', help="Utilizza viste materializzate"
        )
        parser.add_argument('--view_schema', action='store', default=(DEFAULT_SCHEMA or 'osm_views'),
            dest='view_schema', help="Nome dello schema delle viste"
        )
        parser.add_argument('--data_schema', action='store', default='public',
            dest='data_schema', help="Nome dello schema che contiene i dati osmosis"
        )
        parser.add_argument('--views', action='store', default='*', 
            dest='views', help="Vista/e da elaborare; si possono usare i caratteri jolly * e ?"
        )
        parser.add_argument('--post-sql', action='store', default='', 
            dest='post_sql', help="Codice SQL da eseguire al termine delle operazioni"
        )
        parser.add_argument('--owner', action='store', default='', 
            dest='owner', help="Proprietario della vista"
        )
        parser.add_argument('--grant-select', action='store', default='', 
            dest='grant_select', help="CSV dei role a cui dare il select"
        )
        
        self.args, self.extras = parser.parse_known_args()
        self.args.__dict__['db_config'] = DB_CONF_SPATIALITE if self.args.spatialite else DB_CONF_OSMOSIS
            
        
        
    def connect_db(self):
        try:
            self.db_conn_config =  {
                    'host'       : os.environ['DB_HOST'],
                    'port'       : os.environ.get('DB_PORT', '5432'),
                    'database'   : os.environ['DB_NAME'],
                    'user'       : os.environ['DB_USER'],
                    'password'   : os.environ['DB_PASSWORD'],
            }
            self.connection = psycopg2.connect(**self.db_conn_config)
            self.cur = self.connection.cursor()       
        except KeyError as e:
            print("You need to export DB_* variables before calling this script.")
            print("%s environment variable not found." % str(e))
            quit()
            

    def get_views(self):
        """
        Determina su quali viste lavorare; richiesta da tutti i comandi
        """
        self.views = list(RawView.list_view_files())
        if self.args.views:
            self.views = fnmatch.filter(self.views, self.args.views)
        self.oviews = {}
        for view_name in self.views:
            self.oviews[view_name] = RawView(view_name, self.args)
            
            
    def iter_views(self):
        for view in self.views:
            yield self.oviews[view]

        
    def execute_command(self):
        command = self.args.command
        if command in ('drop', 'create'):
            print("Processing views in schema:%s from data in schema:%s..." % (
                    self.args.view_schema, self.args.data_schema)
            )
            self.connect_db()
        for v in self.iter_views():
            if command == 'list':
                print(v.name)
            elif command == 'info':
                print(v)
            else:
                if command == 'sql':
                    print(v.create() + ';')
                    print(v.set_owner() + ';')
                    print(v.set_grant_select() + ';')
                else:
                    if command in ('drop', 'create'):
                        self.drop_view(v)
                    if command == 'create':
                        self.create_view(v)

                    

    def drop_view(self, view):
        print('DROP [%s]' % (view.view_name))
        try:
            self.cur.execute(view.drop())
            self.connection.commit()
        except psycopg2.errors.WrongObjectType:
            self.cur.close()
            self.connection.close()
            self.connect_db()
            self.cur.execute(view.drop(materialized=True))
            self.connection.commit()
            


    def create_view(self, view):
        if view.active:
            status = 'MATERIALIZED' if view.materialized else 'VIEW'
            self.cur.execute(view.create())
            set_owner = view.set_owner()
            if set_owner:
                self.cur.execute(set_owner)
            set_grant_select = view.set_grant_select()
            if set_grant_select:
                self.cur.execute(set_grant_select)
            if self.args.post_sql:
                self.cur.execute(view.translate_sql(self.args.post_sql))
            self.connection.commit()
        else:
            status = 'SKIP'
        print('%s [%s]' % (status, view.view_name))


        
class RawView(object):

    def __init__(self, name, options):
        self.name = name
        self.db_config = options.db_config
        self.filename = self.build_ini_filename()
        self.view_schema = options.view_schema
        self.data_schema = options.data_schema
        self.materialized = self.get_bool_from_str(options.materialized)
        self.owner = options.owner
        self.grant_select = options.grant_select
        try:
            self.load_from_ini(self.filename)
        except Exception as e:
            raise Exception("Error parsing %s\n%s" % (self.filename, str(e)))


    def __str__(self):
        tags = ', '.join(self.tags)
        return f"""
Nome    : {self.name}
Attiva  : {self.active}
Tags    : {tags}
Extra   : {self.extra_fields}

"""

    @classmethod
    def get_view_folder(self):
        return VIEWS_INI_FOLDER or os.path.join(os.path.abspath(os.path.dirname(__file__)), 'views')


    @classmethod
    def list_view_files(cls):
        for viewfile in glob.glob(os.path.join(cls.get_view_folder(), '*.ini')):
            yield os.path.basename(viewfile)[:-4]


    def build_ini_filename(self):
        return os.path.join(RawView.get_view_folder(), self.name + '.ini')


    def load_from_ini(self, filename):
        config = self.parse_ini_file(filename)

        for required_key in ['tags', 'geom_class']:
            if not required_key in config or not config.get(required_key, None):
                raise Exception('Missing required key %s' % required_key)

        self.view_name=config.get('view_name', self.name)
        self.description=config.get('description', '')
        self.meta = self.get_list_from_str(config.get('meta', ''))
        self.extra_fields = self.get_list_from_str(config.get('extra_fields', ''), "|")
        self.default_char_len = config.get('default_char_len', '30')
        self.active = self.get_bool_from_str(config.get('active', '1'))
        if 'materialized' in config:
            self.materialized = self.get_bool_from_str(config.get('materialized', '0'))

        if self.data_schema:
            self.data_schema = '%s.' % self.data_schema.rstrip('.')


        self.geom_class=config['geom_class']
        if not self.geom_class in ('node', 'way'):
            raise Exception('geom_class must be "node" or "way"')
        conf = self.db_config[self.geom_class]

        self.geom_table = conf['geom_table']
        self.tags_table = conf['tags_table']
        self.tags_fk    = conf['tags_fk']
        self.geom_field = conf['geom_field']
        self.use_hstore = conf['use_hstore']

        self.tags = self.get_list_from_str(config['tags'])
        if self.tags == []:
            raise Exception('Specify at least one tag in the tags= property')
        self.where = config.get('where', '').strip()
        self.build_sql()


    def get_list_from_str(self, string_or_list, separator=','):
        if string_or_list.__class__ == str:
            string_or_list = string_or_list.split(separator)
        return [tag.strip() for tag in filter(None, string_or_list)]


    def get_bool_from_str(self, s):
        if not s:
            return False
        return not str(s).lower() in ('no', 'n', '0', 'off', 'false', 'x', 'f')


    def parse_ini_file(self, filename):
        """
        Barebone ini parser with no support for sections
        TODO use inifile
        [extra_fields]
        geom_length=st_length(...
        geom_area=...
        """
        f = open(filename)
        config = {}
        for line in f:
            line = line.strip()
            if line and line[0] != '#':
                (key, value) = (part.strip() for part in line.split('=', 1))
                if value != '':
                    config[key] = value
        f.close()
        return config


    def build_sql(self):
        fields = []
        joins = []
        field_list = []
        wheres = []
        widths = {}
        for field_raw in self.tags:
            fs = field_raw.split('::')
            field_raw = fs[0]
            if len(fs) > 1:
                width = int(fs[1])
            else:
                width = self.default_char_len
            if field_raw[-1] == '*':
                field_raw = field_raw[:-1].strip()
                wheres.append("{table}.tags ? '{tag}'".format(table=self.geom_table, tag=field_raw))
                join_type = 'inner'
            else:
                join_type = 'left'
            widths[field_raw] = width
            field = field_raw.replace(':','__')
            if self.use_hstore:
                fields.append("{table}.tags -> '{field_raw}' as {field}".format(field=field, table=self.geom_table, field_raw=field_raw))
            else:
                fields.append('{field}.v as {field}'.format(field=field))
                join_tpl = "{join_type} join {tags} as {field} on {table}.{tag_id} = {field}.{tag_id} and {field}.k = '{field_raw}'"
                joins.append(join_tpl.format(tags=self.tags_table, tag_id=self.tags_fk, field=field, join_type=join_type, field_raw=field_raw, table=self.geom_table))
            field_list.append('%s :: character varying (%s)' % (field, width))
        mft = ', '.join([self.geom_table + '.' + self.pg_field(meta_field) for meta_field in self.meta if meta_field])
        if mft != '':
            mft += ', '
        extra = ", ".join(self.extra_fields)
        if extra:
            extra += ', '
        if self.use_hstore and len(wheres):
            sql_where = 'where (%s)' % ' and '.join(["(%s)" % w for w in wheres])
        else:
            sql_where = ''
        sql = 'select {table}.{fk}, {meta_fields}{table}.{geom_field}, {fields} from {schema}{table} {joins} {sql_where}'.format(fk=self.tags_fk, fields=','.join(fields),table=self.geom_table,joins=' '.join(joins), meta_fields=mft, schema=self.data_schema, geom_field=self.geom_field, sql_where=sql_where)
        if self.where:
            mft = ', '.join([self.pg_field(mfld) for mfld in self.meta])
            if mft != '':
                mft = mft + ', '
            sql = 'select {fk}::character varying(16), {meta_fields}{extra_fields}{geom_field} as geom, {fields} from ({sql}) as q where ({where})'.format(fk=self.tags_fk, fields=','.join(field_list), sql=sql, where=self.where, meta_fields=mft, geom_field=self.geom_field, extra_fields=extra)
        return sql


    def pg_field(self, raw_field, typecast=True):
        if raw_field[-1] == '*':
            raw_field = raw_field[:-1]
        fldc = raw_field.split('::')
        if len(fldc) > 1 and typecast:
            return "%s::character varying(%s)" % (fldc[0], fldc[1])
        return fldc[0]


    def view_type(self):
        return 'materialized view' if self.materialized else 'view'

    def create(self):
        if self.active:
            return "create %s %s.%s as (%s);" % (self.view_type(), self.view_schema, self.view_name, self.build_sql())
        return ''
    
    
    def set_owner(self):
        if self.owner:
            return "alter table %s.%s owner to %s" % (self.view_schema, self.view_name, self.owner)
        return ''


    def set_grant_select(self):
        if self.grant_select:
            return "grant select on table %s.%s to %s" % (self.view_schema, self.view_name, self.grant_select)
        return ''


    def drop(self, materialized=False):
        mat = 'materialized' if materialized else ''
        return 'drop {mat} view if exists {schema}.{view} cascade; '.format(mat=mat, schema=self.view_schema, view=self.view_name)


    def translate_sql(self, sql):
        return sql.format(
            view_type=self.view_type(),
            view_name='%s.%s' % (self.view_schema, self.view_name)
        )



if __name__ == '__main__':
    
    app = App()
    app.run()

