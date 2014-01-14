import posixpath
import collections
import numpy as np
from os.path import join, relpath
import pandas as pd
from pandas.io import sql
import json
import logging
import itertools

from arraymanagement.nodes.hdfnodes import (PandasCacheableTable, 
                                            write_pandas_hdf_from_cursor,
                                            write_pandas,
                                            override_hdf_types,
                                            )
from arraymanagement.nodes.hdfnodes import Node
from arraymanagement.nodes.sql import query_info

from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql import column, and_
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)



class DumbParameterizedQueryTable(PandasCacheableTable):
    config_fields = ['query',
                     
                     #args to pass into connect
                     'sqlalchemy_args',
                     
                     #args to pass into connect                     
                     'sqlalchemy_kwargs',
                     
                     #types in cursor description which represent
                     'db_string_types',
                     
                     #types in cursor description which are datetime types
                     'db_datetime_types',
                     
                     #column name to type mappings
                     'col_types',
                     
                     # minimum column sizes 
                     'min_itemsize',
                     
                     'cache_discrete_fields',
                     
                     'cache_continuous_fields',
                     ]
    
    def __init__(self, *args, **kwargs):
        super(DumbParameterizedQueryTable, self).__init__(*args, **kwargs)
        if self.query is None:
            self.init_from_file()
        self.engine = create_engine(*self.sqlalchemy_args, 
                                    **self.sqlalchemy_kwargs)
        self.session = sessionmaker(bind=self.engine)()
        
    def init_from_file(self):
        with open(join(self.basepath, self.relpath)) as f:
            data = f.read()
            query, discrete_fields, continuous_fields = data.split("---")
            discrete_fields = [x.strip() for x in discrete_fields.split(",") \
                                   if x.strip()]
            continuous_fields = [x.strip() for x in continuous_fields.split(",") \
                                     if x.strip()]
            self.query = query
            self.cache_discrete_fields = discrete_fields
            self.cache_continuous_fields = continuous_fields
            
    def query_min_itemsize(self):
        try:
            min_itemsize = self.store.select('min_itemsize')
        except KeyError:
            return None
        return min_itemsize.to_dict()
    
    @property
    def min_itemsize(self):
        min_itemsize = self.query_min_itemsize()
        if min_itemsize is None:
            return None
        else:
            min_itemsize.pop('finalized', None)
            return min_itemsize
    
    @min_itemsize.setter
    def min_itemsize(self, val):
        if val is None:
            return
        min_itemsize = self.query_min_itemsize()
        if min_itemsize is not None and min_itemsize['finalized']:
            logger.debug("will not set min_itemsize, already finalized")
        else:
            if 'finalized' not in val:
                val['finalized'] = False
            val = pd.Series(val)
            self.store.put('min_itemsize', val)
            self.store.flush()

    def finalize_min_itemsize(self):
        min_itemsize = self.query_min_itemsize()
        min_itemsize['finalized'] = True
        self.min_itemsize = min_itemsize
        
    def cache_spec_min_itemsize(self):
        min_itemsize = self.min_itemsize.copy()
        for field in self.cache_continuous_fields:
            if field in min_itemsize:
                min_itemsize[field + "_start"] = min_itemsize[field]
                min_itemsize[field + "_end"] = min_itemsize[field]
        return min_itemsize

    def cache_spec_col_types(self):
        col_types = self.col_types.copy()
        for field in self.cache_continuous_fields :
            if field in col_types:
                col_types[field + "_start"] = col_types[field]
                col_types[field + "_end"] = col_types[field]
        return col_types

    def store_cache_spec(self, query_params, start_row, end_row):
        data = pd.DataFrame(self.parameter_dict(query_params), index=[0])
        data['_start_row'] = start_row
        data['_end_row'] = end_row
        min_itemsize = self.cache_spec_min_itemsize()
        overrides = self.cache_spec_col_types()
        data = override_hdf_types(data, overrides)
        write_pandas(self.store, 'cache_spec', data, min_itemsize, 1.1,
                     replace=False)
        
    def cache_info(self, query_params):
        param_dict = self.parameter_dict(query_params)
        query = param_dict.items()
        try:
            result = self.store.select('cache_spec', where=query)
        except KeyError:
            return None
        if result is None:
            return None
        if result.shape[0] == 0:
            return None
        else:
            return result['_start_row'], result['_end_row']

    def parameter_dict(self, query_params):
        output = collections.OrderedDict()
        for field in self.cache_discrete_fields:
            output[field] = query_params[field]
        for field in self.cache_continuous_fields:
            val_range = query_params[field]
            output[field + "_start"] = query_params[field][0]
            output[field + "_end"] = query_params[field][1]
        return output
    
    def filter_sql(self, **kwargs):
        clauses = []
        for field in self.cache_discrete_fields:
            clauses.append(column(field) == bindparam(field))
        for field in self.cache_continuous_fields:
            val_range = kwargs[field]
            name = field + "_start"
            clauses.append(column(field) >= bindparam(name))
            name = field + "_end"
            clauses.append(column(field) <= bindparam(name))
        return and_(*clauses)
    
    def cache_query(self, query_params):
        params = self.parameter_dict(query_params)
        q = """select * from (%s) as X where %s"""
        filter_clause = self.filter_sql(**query_params)
        filter_clause = str(filter_clause)
        q = q % (self.query, filter_clause)
        return q
    
    def cache_data(self, query_params):
        q = self.cache_query(query_params)
        print q
        params = self.parameter_dict(query_params)
        print params
        cur = self.session.execute(q, params)
        
        min_itemsize = self.min_itemsize if self.min_itemsize else {}
        
        db_string_types = self.db_string_types if self.db_string_types else []
        db_datetime_types = self.db_datetime_types if self.db_datetime_types else []
        
        #hack
        cur.description = cur._cursor_description()
        cur.arraysize = 500

        columns, min_itemsize, dt_fields = query_info(
            cur,
            min_itemsize=min_itemsize,
            db_string_types=db_string_types,
            db_datetime_types=db_datetime_types
            )
        
        self.min_itemsize = min_itemsize
        self.finalize_min_itemsize()
        overrides = self.col_types
        for k in dt_fields:
            overrides[k] = 'datetime64[ns]'
        try:
            starting_row = self.table.nrows
        except AttributeError:
            starting_row = 0
        write_pandas_hdf_from_cursor(self.store, self.localpath, cur, 
                                     columns, self.min_itemsize, 
                                     dtype_overrides=overrides,
                                     min_item_padding=self.min_item_padding,
                                     chunksize=50000, 
                                     replace=False)
        try:
            ending_row = self.table.nrows
        except AttributeError:
            ending_row = 0
        self.store_cache_spec(query_params, starting_row, ending_row)

    def load_data(self):
        raise NotImplementedError
    
    def hdfstore_selection(self, **kwargs):
        where = []
        for field in self.cache_discrete_fields:
            where.append((field, kwargs.pop(field)))
        for field in self.cache_continuous_fields:
            val_range = kwargs.pop(field)
            where.append(field, "<=", val_range[0])
            where.append(field, "<=", val_range[1])
        where += kwargs.pop('where', [])
        return where
    
    def select(self, **kwargs):
        for field in self.cache_discrete_fields:
            if not isinstance(kwargs.get(field), (list, tuple, np.ndarray)):
                kwargs[field] = [kwargs.get(field)]
        vals = [kwargs.get(x) for x in self.cache_discrete_fields]
        discrete_vals = itertools.product(*vals)
        query_params = []
        for discrete_val in discrete_vals:
            query_param = dict(zip(self.cache_discrete_fields, discrete_val))
            for field in self.cache_continuous_fields:
                query_param[field] = kwargs.get(field)
            query_params.append(query_param)
        results = []
        for query_param in query_params:
            result = self._single_select(where=kwargs.get('where', None),
                                         **query_param)
            results.append(result)
        return pd.concat(results)

    def _single_select(self, **kwargs):
        query_params = kwargs
        where = query_params.pop('where')
        cache_info = self.cache_info(query_params)
        if cache_info is None:
            self.cache_data(query_params)
            cache_info = self.cache_info(query_params)
        start_row, end_row = cache_info
        if not where:
            where = None
        result = self.store.select(self.localpath, where=where,
                                   start=start_row, stop=end_row)
        return result
    def repr_data(self):
        repr_data = super(DumbParameterizedQueryTable, self).repr_data()
        repr_data.append("query: %s" % self.query)
        repr_data.append("discrete_fields: %s" % self.cache_discrete_fields)
        repr_data.append("continuous_fields: %s" % self.cache_continuous_fields)
        discrete_args = ["%s=%s" % (x, x) for x in self.cache_discrete_fields]
        continuous_args = ["%s=[%s_start, %s_end]" % (x, x, x) \
                               for x in self.cache_continuous_fields]
        args = discrete_args + continuous_args + ["where=[extra_terms]"]
        args = ", ".join(args)
        cmd = "select(%s)" % args
        repr_data.append("syntax: %s" % cmd)
        return repr_data
