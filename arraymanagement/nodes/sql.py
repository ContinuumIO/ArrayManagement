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
        return mod.connect(*self.db_conn_args)

    def execute_query_df(self, query=None):
        if query is None:
            query = self.query
        with self.db() as db:
            return sql.read_frame(query, db)
    
    def execute_query(self, query=None):
        if query is None:
            query = self.query
        with self.db() as db:
            cur = db.cursor()
            cur.execute(self.query)
            return cur

    def load_data(self):
        store = self.store
        logger.debug("query executing!")
        cur = self.execute_query()
        logger.debug("query returned!")
        logger.debug("cursor descr %s", cur.description)
        
        min_itemsize = self.min_itemsize if self.min_itemsize else {},
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
        overrides = self.config.get('table_type_overrides').get(self.key, {})
        datetime_type = self.config.get('datetime_type')
        dt_overrides = overrides.setdefault(datetime_type, [])
        dt_overrides += dt_fields
        write_pandas_hdf_from_cursor(self.store, self.localpath, cur, 
                                     columns, self.min_itemsize, 
                                     dtype_overrides=overrides,
                                     min_item_padding=self.min_item_padding,
                                     chunksize=50000, 
                                     replace=True)
        cur.close()
        self.store.flush()
