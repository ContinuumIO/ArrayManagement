from arraymanagement.nodes.csvnodes import PandasCSVNode
from arraymanagement.nodes.hdfnodes import PandasHDFNode
from arraymanagement.nodes.hdfnodes import PandasHDFNode, PyTables
from arraymanagement.nodes.sql import SimpleQueryTable

global_config = dict(
    is_dataset = False,
    csv_options = {},
    datetime_type = 'datetime64[ns]',
    loaders = {
        '*.csv' : PandasCSVNode,
        '*.CSV' : PandasCSVNode,
        '*.pandas' : PandasHDFNode,
        '*.h5' : PyTables,
        '*.hdf5' : PyTables,
        '*.sql' : SimpleQueryTable,
        },
    )            

local_config = {
    }

