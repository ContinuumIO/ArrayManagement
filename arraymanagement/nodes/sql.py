import posixpath
from os.path import join, relpath
import pandas as pd
from pandas.io import sql
import logging

import cx_Oracle

from arraymanagement.nodes.hdfnodes import PandasCacheableTable, write_pandas_hdf_from_cursor


logger = logging.getLogger(__name__)

class SimpleQueryTable(PandasCacheableTable):
    is_group = False
    def __init__(self, *args, **kwargs):
        self.query = kwargs.pop('query')
        super(SimpleQueryTable, self).__init__(*args, **kwargs)

    def execute_query(self):
        mod = self.config.get('db_module')
        with mod.connect(*self.config.get('db_conn_args')) as db:
            cur = db.cursor()
            cur.execute(self.query)
            return cur

    def query_info(self, cur):
        min_itemsize = {}
        columns = []
        dt_fields = []
        for col_desc in cur.description:
            name = col_desc[0]
            dtype = col_desc[1]
            length = col_desc[3]
            if dtype in self.config.get('db_string_types'):
                min_itemsize[name] = length
            if dtype in self.config.get('db_datetime_types'):
                dt_fields.append(name)
            columns.append(name)
        return columns, min_itemsize, dt_fields

    def load_data(self, force=False, batch=False):
        store = self._get_store()
        if not force and self.localpath in store.keys():
            return
        logger.debug("query executing!")
        cur = self.execute_query()
        logger.debug("query returned!")
        columns, min_itemsize, dt_fields = self.query_info(cur)
        self.min_itemsize = min_itemsize
        logger.debug("queryinfo %s", str((columns, min_itemsize, dt_fields)))
        if batch:
            logger.debug("batching results!")
            cur = cur.fetchall()
            logger.debug("batching done!")
        write_pandas_hdf_from_cursor(self.store, self.localpath, cur, columns, self.min_itemsize, 
                                     dt_fields,
                                     min_item_padding=self.min_item_padding,
                                     chunksize=50000, 
                                     replace=True)
        self.store.flush()

class SimpleParameterizedQueryTable(SimpleQueryTable):
    @property
    def query(self):
        key = posixpath.basename(self.urlpath)
        return self._query % key

    @query.setter
    def querysetter(self, val):
        self._query = val

    def execute_query(self):
        key = posixpath.basename(self.urlpath)
        mod = self.config.get('db_module')
        with mod.connect(*self.config.get('db_conn_args')) as db:
            cur = db.cursor()
            cur.execute(self.query)
            return cur
