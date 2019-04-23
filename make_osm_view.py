#! /usr/bin/python

import sys
import os.path
import glob

DB_CONF = {
    'spatialite' : {
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
    },

    'osmosis_pg' : {
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
}
class RawView(object):

    def __init__(self, name, options, db_config=''):
        self.name = name
        self.db_config = db_config
        self.filename = self.build_ini_filename()
        self.view_schema = options['view_schema']
        self.data_schema = options['data_schema']
        self.materialized = self.get_bool_from_str(options['materialized'])
        try:
            self.load_from_ini(self.filename)
        except Exception, e:
            raise Exception("Error parsing %s\n%s" % (self.filename, str(e)))


    @classmethod
    def get_view_folder(self):
        return os.path.join(os.path.abspath(os.path.dirname(__file__)), 'views')


    @classmethod
    def list_view_files(cls):
        for viewfile in glob.glob(os.path.join(cls.get_view_folder(), '*.ini')):
            yield viewfile[:-4]


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
        conf = DB_CONF[self.db_config][self.geom_class]

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


    def drop(self, materialized=''):
        return 'drop %s view if exists %s.%s cascade;' % (materialized, self.view_schema, self.view_name)


    def translate_sql(self, sql):
        return sql.format(
            view_type=self.view_type(),
            view_name='%s.%s' % (self.view_schema, self.view_name)
        )


if __name__ == '__main__':
    try:
        view = sys.argv[1]
        if len(sys.argv) > 2:
            db_config = sys.argv[2]
        else:
            db_config = 'spatialite'
    except:
        print "Usage: %s <view> [spatialite|osmosis_pg]" % sys.argv[0]
        quit()
    rview = RawView(view, {'materialized': 0}, db_config=db_config)
    print rview.build_sql()
