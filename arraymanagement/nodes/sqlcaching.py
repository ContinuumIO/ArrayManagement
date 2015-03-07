import posixpath
import collections
import numpy as np
from os.path import join, relpath
import pandas as pd
from pandas.io import sql
import json
import itertools
import pickle
import hashlib
from .nodes.hdfnodes import (PandasCacheableTable,
                                            write_pandas_hdf_from_cursor,
                                            write_pandas,
                                            override_hdf_types,
                                            )
from .nodes.hdfnodes import Node, store_select
from .nodes.sql import query_info

from sqlalchemy.sql.expression import bindparam, tuple_
from sqlalchemy.sql import column, and_
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from .logger import log

import yaml
import tables
import sys
from pdb import set_trace
from collections import defaultdict

#suppress pytables warnings
import warnings
warnings.filterwarnings('ignore',category=pd.io.pytables.PerformanceWarning)
warnings.filterwarnings('ignore',category=tables.NaturalNameWarning)

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
            query, discrete_fields, continuous_fields = data.split(SEP)
            discrete_fields = [x.strip() for x in discrete_fields.split(",") \
                                   if x.strip()]
            continuous_fields = [x.strip() for x in continuous_fields.split(",") \
                                     if x.strip()]
            self.query = query
            self.cache_discrete_fields = discrete_fields
            self.cache_continuous_fields = continuous_fields

    def query_min_itemsize(self):
        try:
            min_itemsize = store_select(self.store, 'min_itemsize')
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
            pass
            # log.debug("will not set min_itemsize, already finalized")
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
        output = {}
        for field in self.cache_continuous_fields:
            if field in min_itemsize:
                output[field + "_start"] = min_itemsize[field]
                output[field + "_end"] = min_itemsize[field]
        for field in self.cache_discrete_fields:
            if field in min_itemsize:
                output[field] = min_itemsize[field]
        return output

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
            result = store_select(self.store, 'cache_spec', where=query)
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
            clauses.append(column(field) == kwargs[field])
        for field in self.cache_continuous_fields:
            val_range = kwargs[field]
            clauses.append(column(field) >= val_range[0])
            clauses.append(column(field) <= val_range[1])
        return and_(*clauses)

    def cache_query(self, query_params):
        q = """ * from (%s) as X """
        q = q % self.query
        filter_clause = self.filter_sql(**query_params)
        query = self.session.query(q).filter(filter_clause)
        return query

    def cache_data(self, query_params):
        q = self.cache_query(query_params)
        log.debug(str(q))


        cur = self.session.execute(q)

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
        where = query_params.pop('where', None)
        cache_info = self.cache_info(query_params)
        if cache_info is None:
            self.cache_data(query_params)
            cache_info = self.cache_info(query_params)
        start_row, end_row = cache_info
        #convert these series to ints
        start_row = start_row[0]
        end_row = end_row[0]
        if not where:
            where = None
        result = store_select(self.store, self.localpath,
                              where=where, start=start_row, stop=end_row)
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

def gethashval(obj):
    m = hashlib.md5()
    m.update(pickle.dumps(obj))
    return m.hexdigest()

class BulkParameterizedQueryTable(DumbParameterizedQueryTable):
    def select(self, **kwargs):
        for field in self.cache_discrete_fields:
            if not isinstance(kwargs.get(field), (list, tuple, np.ndarray)):
                kwargs[field] = [kwargs.get(field)]
        query_params = kwargs
        where = query_params.pop('where', None)
        cache_info = self.cache_info(query_params)
        if cache_info is None:
            self.cache_data(query_params)
            cache_info = self.cache_info(query_params)
        start_row, end_row = cache_info
        if not where:
            where = None
        result = store_select(self.store, self.localpath,
                              where=where, start=start_row, stop=end_row)
        return result

    def filter_sql(self, **kwargs):
        clauses = []
        for field in self.cache_discrete_fields:
            clauses.append(column(field).in_(kwargs[field]))
        for field in self.cache_continuous_fields:
            val_range = kwargs[field]
            clauses.append(column(field) >= val_range[0])
            clauses.append(column(field) <= val_range[1])
        return and_(*clauses)

    def store_cache_spec(self, query_params, start_row, end_row):
        data = self.parameter_dict(query_params)
        hashval = gethashval(data)
        data = pd.DataFrame({'hashval' : [hashval],
                             '_start_row' : start_row,
                             '_end_row' : end_row})
        write_pandas(self.store, 'cache_spec', data, {}, 1.1,
                     replace=False)

    def cache_info(self, query_params):
        data = self.parameter_dict(query_params)
        hashval = gethashval(data)
        try:
            result = store_select(self.store, 'cache_spec',
                                  where=[('hashval', hashval)])
        except KeyError:
            return None
        if result is None:
            return None
        if result.shape[0] == 0:
            return None
        else:
            return result['_start_row'], result['_end_row']

class FlexibleSqlCaching(BulkParameterizedQueryTable):
    def __init__(self,other_params):
        self.__dict__.update(other_params.__dict__)

    def init_from_file(self):
        with open(join(self.basepath, self.relpath)) as f:
            data = yaml.load(f)
            assert len(data['SQL'].keys()) == 1
            key = data['SQL'].keys()[0]
            query = data['SQL'][key]['query']
            fields = data['SQL'][key]['conditionals']

            self.query = query
            self.fields = fields


    def select(self, query_filter, where=None):
        cache_info = self.cache_info(query_filter)
        if cache_info is None:
            self.cache_data(query_filter)
            cache_info = self.cache_info(query_filter)

        start_row, end_row = cache_info
        result = store_select(self.store, self.localpath, where=where,
                              start=start_row, stop=end_row)
        return result

    def cache_query(self, query_filter):
        q = """ * from (%s) as X """
        q = q % self.query
        if not hasattr(query_filter,'compile'):
            query = self.session.query(q)
        else:
            query = self.session.query(q).filter(query_filter)
        return query

    def gethashval(self, query_filter):

        if not hasattr(query_filter,'compile'):
            hashval = gethashval(('',''))
        else:
            hashval = gethashval((str(query_filter),
                              query_filter.compile().construct_params()))
        return hashval

    def store_cache_spec(self, query_filter, start_row, end_row):
        hashval = self.gethashval(query_filter)
        data = pd.DataFrame({'hashval' : [hashval],
                             '_start_row' : start_row,
                             '_end_row' : end_row})
        write_pandas(self.store, 'cache_spec', data, {}, 1.1,
                     replace=False)


    def cache_info(self, query_filter):
        hashval = self.gethashval(query_filter)
        try:
            #rewriting where statement for 0.13 pandas style
            result = store_select(self.store, 'cache_spec',
                                  where=[('hashval', hashval)])
        except KeyError:
            return None
        if result is None:
            return None
        if result.shape[0] == 0:
            return None
        else:
            return result['_start_row'], result['_end_row']
    def repr_data(self):
        repr_data = super(DumbParameterizedQueryTable, self).repr_data()
        repr_data.append("query: %s" % self.query)
        repr_data.append("fields: %s" % self.fields)
        return repr_data


class YamlSqlDateCaching(BulkParameterizedQueryTable):
    def init_from_file(self):
        with open(join(self.basepath, self.relpath)) as f:
            data = yaml.load(f)
            assert len(data['SQL'].keys()) == 1

            key = data['SQL'].keys()[0]
            query = data['SQL'][key]['query']
            if 'conditionals' in data['SQL'][key].keys():
                fields = data['SQL'][key]['conditionals']
            else:
                fields = None

            self.query = query
            self.fields = fields

            #no conditionals defined
            if fields is not None:
                for f in fields:
                    name = f.lower()
                    setattr(self, name, column(name))

    def select(self, query_filter, where=None, **kwargs):
        ignore_cache = kwargs.get('IgnoreCache',None)
        if ignore_cache:
            query = self.compiled_query(query_filter,kwargs)
            return query

        dateKeys = [k for k in kwargs.keys() if 'date' in k]
        if not dateKeys:
            #no dates in query

            fs = FlexibleSqlCaching(self)
            fs.localpath = self.localpath
            fs.urlpath = self.urlpath
            fs.store = self.store
            result = fs.select(query_filter)
            return result

        else:
            dateKeys = sorted(dateKeys)
            start_date, end_date = kwargs[dateKeys[0]], kwargs[dateKeys[1]]

            result = self.cache_info(query_filter,start_date, end_date)

            if result is None:
                self.cache_data(query_filter, start_date, end_date)
                result = self.cache_info(query_filter,start_date, end_date)

        return result

    def cache_query(self, query_filter):
        q = """ * from (%s) as X """
        q = q % self.query
        query = self.session.query(q).filter(query_filter)
        return query

    def gethashval(self, query_filter):
        hashval = gethashval((str(query_filter),
                              query_filter.compile().construct_params()))
        return hashval
    def store_cache_spec(self, query_filter, start_row, end_row, start_date, end_date):
        hashval = self.gethashval(query_filter)
        data = pd.DataFrame({'hashval' : [hashval],
                             '_start_row' : start_row,
                             '_end_row' : end_row,
                             'start_date': start_date,
                             'end_date': end_date,})
        write_pandas(self.store, 'cache_spec', data, {}, 1.1,
                     replace=False)

    def cache_info(self, query_filter, start_date, end_date):
        hashval = self.gethashval(query_filter)
        try:
            # print self.store['/cache_spec']
            # result = store_select(self.store, 'cache_spec',
            #                       where=[('hashval', hashval),
            #                              ('start_date',start_date)])

            start_date = pd.Timestamp(start_date)
            end_date = pd.Timestamp(end_date)

            cache_spec = self.store['cache_spec']
            cache_spec = cache_spec[cache_spec.hashval == hashval]

            if len(cache_spec) == 0: return None

            max_date = cache_spec['end_date'].max()
            min_date = cache_spec['start_date'].min()
            start_date = pd.Timestamp(start_date)
            end_date = pd.Timestamp(end_date)

            max_date = self.store['/cache_spec']['end_date'].max()
            min_date = self.store['/cache_spec']['start_date'].min()

            #inner selection
            if (start_date >= min_date) and (end_date <= max_date):

                result = self.munge_tables(hashval, start_date,end_date)
                return result

            #left shift
            elif (start_date < min_date) and (end_date <= max_date):
                return self.shift_left(query_filter, hashval, start_date,end_date)

            #right shift
            elif (start_date >= min_date) and (end_date > max_date):
                return self.shift_right(query_filter, hashval, start_date,end_date)

            #full_outer
            elif (start_date < min_date) and (end_date > max_date):
                right = self.shift_right(query_filter, hashval, start_date,end_date)
                left = self.shift_left(query_filter, hashval, start_date,end_date)
                data = right.append(left)
                data.reset_index(inplace=True)
                data = data.drop(['index'],axis=1)
                return data

            else:
                print 'something terrible has gone wrong'
                print query_filter, hashval, start_date,end_date, max_date, min_date


        except KeyError:
            return None
        if result is None:
            return None
        if result.shape[0] == 0:
            return None
        else:
            return result['_start_row'], result['_end_row']

    def repr_data(self):
        repr_data = super(DumbParameterizedQueryTable, self).repr_data()
        repr_data.append("query: %s" % self.query)
        repr_data.append("fields: %s" % self.fields)
        return repr_data

    def cache_data(self, query_params, start_date, end_date):

        for f in self.fields:
            if 'date' in f:
                col_date = f
                break;

        all_query = and_(query_params,column(col_date) >=start_date, column(col_date) <= end_date)
        q = self.cache_query(all_query)
        log.debug(str(q))

        cur = self.session.execute(q)

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
        self.store_cache_spec(query_params, starting_row, ending_row, start_date, end_date)


    def munge_tables(self, hashval, start_date, end_date):

        store = self.store
        # store.select('cache_spec', where=[('hashval', hashval)])

        store['/cache_spec'][['start_date','end_date']].sort(['start_date'])
        df_min = store_select(store, 'cache_spec', where=[('start_date', '<=', start_date)]).reset_index()
        df_max = store_select(store, 'cache_spec', where=[('end_date', '<=', end_date)]).reset_index()

        df_total = df_min.append(df_max)
        df_total.drop_duplicates('_end_row',inplace=True)
        df_total.reset_index(inplace=True)

        ss_vals = df_total[['_start_row','_end_row', ]].values

        df_list = []
        for s in ss_vals:
            start_row = s[0]
            end_row = s[1]
            temp = store_select(store, self.localpath,
                                           start=start_row, stop=end_row)
            temp.head()

            df_list.append(temp)

        df_concat = pd.concat(df_list)
        df_concat.sort(['date'],inplace=True)

        df_return = df_concat[(df_concat['date'] >= start_date) & (df_concat['date'] <= end_date)]

        return df_return


    def shift_right(self,query_filter, hashval, start_date,end_date):
        '''
        the query contains date closer in time than the max.
        fetch data from current max to end_data
        '''
        max_date = self.store['/cache_spec']['end_date'].max()

        max_date = max_date.to_datetime()

        start_date = start_date.to_datetime()
        end_date = end_date.to_datetime()

        self.cache_data(query_filter, max_date, end_date)

        return self.munge_tables(hashval, start_date, end_date)

    def shift_left(self,query_filter, hashval, start_date,end_date):
        '''
        the query contains date further in time than the minimum.
        fetch data from current max to end_data
        '''
        min_date = self.store['/cache_spec']['start_date'].min()

        min_date = min_date.to_datetime()

        start_date = start_date.to_datetime()
        end_date = end_date.to_datetime()

        self.cache_data(query_filter, start_date, min_date)

        return self.munge_tables(hashval, start_date, end_date)

    def compiled_query(self,query_params,kwargs):
        if 'date' not in kwargs.keys():
            all_query = and_(query_params)
            result = self.cache_query(all_query)
            return str(result)

        else:
            dateKeys = [k for k in kwargs.keys() if 'date' in k]
            dateKeys = sorted(dateKeys)
            start_date, end_date = kwargs[dateKeys[0]], kwargs[dateKeys[1]]

            for f in self.fields:
                if 'date' in f:
                    col_date = f
                    break;

            all_query = and_(query_params,column(col_date) >=start_date, column(col_date) <= end_date)

            result = self.cache_query(all_query)

            return str(result)
