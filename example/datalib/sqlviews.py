import arraymanagement.nodes.sql
reload(arraymanagement.nodes.sql)
from arraymanagement.nodes.sql import SimpleQueryTable
from arraymanagement.nodes.sqlcaching import (YamlSqlDateCaching,
                                              )
import collections
import sys
from os.path import dirname, join, abspath
import sqlite3
db_file = abspath(join(dirname(__file__), "data.db"))
loaders = collections.OrderedDict([
        ("*.yaml", YamlSqlDateCaching),
        ])
config = {
    'loaders' : loaders,
    'db_module' : sqlite3,
    'db_conn_args' : (db_file,),
    'db_conn_kwargs' : {
        'detect_types' : sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES
    },
    'sqlalchemy_args' : ["sqlite:///" + db_file],
    'sqlalchemy_kwargs' : {
        'connect_args' : {
            'detect_types' : sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES
            }
        },
    'col_types' : {'ticker' : 'S10'},
    '__module__' : sys.modules[__name__]
}
