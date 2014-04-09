import collections
from arraymanagement.nodes.csvnodes import PandasCSVNode
from arraymanagement.nodes.hdfnodes import PandasHDFNode, PyTables
from arraymanagement.nodes.sql import SimpleQueryTable
from arraymanagement.nodes.sqlcaching import YamlSqlDateCaching


global_config = dict(
    is_dataset = False,
    csv_options = {},
    datetime_type = 'datetime64[ns]',
    loaders = collections.OrderedDict([
        ('*pytables/*.hdf5', PyTables),
        ('*.csv' , PandasCSVNode),
        ('*.CSV' , PandasCSVNode),
        ('*.hdf5' , PandasHDFNode),
        ('*.h5' , PandasHDFNode),
        ('*.sql' , SimpleQueryTable),
        ("*.yaml", YamlSqlDateCaching),
        ])
    )            

import custom
import customcsvs
import sqlviews

local_config = {
    '/custom' : custom.config,
    '/customcsvs' : customcsvs.config,
    '/sqlviews' : sqlviews.config
    }

