import posixpath
from os.path import join, relpath
import pandas as pd
from pandas.io import sql
import logging

from arraymanagement.nodes.hdfnodes import PandasCacheableTable, write_pandas_hdf_from_cursor


logger = logging.getLogger(__name__)

def query_info(cur, min_itemsize, db_string_types, db_datetime_types):
    columns = []
    dt_fields = []
    for col_desc in cur.description:
        name = col_desc[0]
        dtype = col_desc[1]
        length = col_desc[3]
        if dtype in db_string_types:
            if length:
                min_itemsize[name] = length
        if dtype in db_datetime_types:
            dt_fields.append(name)
        columns.append(name)
    return columns, min_itemsize, dt_fields
class SimpleQueryTable(PandasCacheableTable):
    is_group = False
    config_fields = [
        'query',
        #args to pass into connect
        'db_module',
        #args to pass into connect                     
        'db_conn_args',
        'db_conn_kwargs',
        #types in cursor description which represent
        'db_string_types',
        #types in cursor description which are datetime types
        'db_datetime_types',
        #column name to type mappings
        'col_types',
        # minimum column sizes 
        'min_itemsize',
        ]

    def __init__(self, *args, **kwargs):
        query = None
        if 'query' in kwargs:
            query = kwargs.pop('query')
        super(SimpleQueryTable, self).__init__(*args, **kwargs)
        if query:
            self.query = query
        else:
            with open(join(self.basepath, self.relpath)) as f:
                self.query = f.read()

    def db(self):
        mod = self.db_module
        return mod.connect(*self.db_conn_args, **self.db_conn_kwargs)

    def execute_query_df(self, query=None):
        if query is None:
            query = self.query
        with self.db() as db:
            return sql.read_frame(query, db)
    
    def load_data(self):
        store = self.store
        with self.db() as db:
            logger.debug("connected db!")
            cur = db.cursor()
            logger.debug("query executing!")
            cur.execute(self.query)
            logger.debug("query returned!")
            logger.debug("cursor descr %s", cur.description)

            min_itemsize = self.min_itemsize if self.min_itemsize else {}
            db_string_types = self.db_string_types if self.db_string_types else []
            db_datetime_types = self.db_datetime_types if self.db_datetime_types else []

            columns, min_itemsize, dt_fields = query_info(
                cur,
                min_itemsize=min_itemsize,
                db_string_types=db_string_types,
                db_datetime_types=db_datetime_types
                )
            self.min_itemsize = min_itemsize
            logger.debug("queryinfo %s", str((columns, min_itemsize, dt_fields)))
            overrides = self.col_types
            for k in dt_fields:
                overrides[k] = 'datetime64[ns]'
            write_pandas_hdf_from_cursor(self.store, self.localpath, cur, 
                                         columns, self.min_itemsize, 
                                         dtype_overrides=overrides,
                                         min_item_padding=self.min_item_padding,
                                         chunksize=50000, 
                                         replace=True)
            cur.close()
            self.store.flush()
