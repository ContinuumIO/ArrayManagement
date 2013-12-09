import collections
from arraymanagement.nodes.csvnodes import PandasCSVNode
from arraymanagement.nodes.hdfnodes import PandasHDFNode
from arraymanagement.nodes.sql import SimpleQueryTable

global_config = dict(
    is_dataset = False,
    csv_options = {},
    table_type_overrides = {},
    datetime_type = 'datetime64[ns]',
    loaders = {
        '*.csv' : PandasCSVNode,
        '*.CSV' : PandasCSVNode,
        '*.hdf5' : PandasHDFNode,
        '*.h5' : PandasHDFNode,
        '*.sql' : SimpleQueryTable,
        },
    pattern_priority = []
    )            

import custom
import customcsvs
local_config = {
    '/custom' : custom.config,
    '/customcsvs' : customcsvs.config
    }

