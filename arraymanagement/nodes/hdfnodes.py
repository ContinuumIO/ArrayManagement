import os
import copy
from os.path import basename, splitext, join, isfile, isdir, dirname, exists
import posixpath
import math

import pandas as pd
import numpy as np
import datetime as dt

from ..exceptions import ArrayManagementException
from ..pathutils import dirsplit
from . import Node
import logging
logger = logging.getLogger(__name__)

class HDFDataGroupMixin(object):
    def put(self, key, *args, **kwargs):
        new_local_path = join(self.localpath, key)
        return self.store.put(new_local_path, *args, **kwargs)

class HDFDataSetMixin(object):
    def select(self, *args, **kwargs):
        return self.store.select(self.localpath, *args, **kwargs)
    
    def append(self, *args, **kwargs):
        return self.store.append(self.localpath, *args, **kwargs)

    def get(self, *args, **kwargs):
        return self.store.get(self.localpath, *args, **kwargs)

pandas_hdf5_cache = {}
def get_pandas_hdf5(path):
    store = pandas_hdf5_cache.get(path)
    if store is None or store._handle.isopen == 0:
        store = pd.HDFStore(path, complib='blosc')
        pandas_hdf5_cache[path] = store
    return store
def hack_pandas_ns_issue(col):
    col[col > dt.datetime(2250,1,1)] = dt.datetime(2250,1,1)
    col = col.astype('datetime64[ns]')
    return col
def override_hdf_types(df, overrides):
    for dtype, override_cols in overrides.iteritems():
        for col in override_cols:
            if col in df:
                if 'datetime' in dtype:
                    df[col] = hack_pandas_ns_issue(df[col])
                else:
                    df[col] = df[col].astype(dtype)
    return df
        
def write_pandas_hdf_from_cursor(store, localpath, cursor, columns, min_itemsize, 
                                 dtype_overrides={},
                                 min_item_padding=1.1, chunksize=500000, replace=True):
    """
    min_itemsize - dict of string columns, to length of string columns.  Cannot specify 'unknown'
    if unknown, we will compute it here
    min_item_padding, for computed min_item_sizes, we will multiply by padding to give
    ourselves some buffer
    """
    logger.debug("writing data to %s with %s columns %s min_itemsize", 
                 localpath, columns, min_itemsize)
    if replace:
        if localpath in store:
            store.remove(localpath)
    data = []
    count = 0
    global_count = 0
    last_global_count = 0
    def write(index):
        if data:
            df = pd.DataFrame.from_records(data, columns=columns, 
                                           index=index)
            override_hdf_types(df, dtype_overrides)
            logger.debug("writing rowend %s", global_count)
            store.append(localpath, df, min_itemsize=min_itemsize, 
                         chunksize=chunksize, data_columns=True)
            store.flush()
    while True:
        d = cursor.fetchmany(cursor.arraysize)
        count += len(d)
        global_count += len(d)
        data.extend(d)
        if len(d) == 0:
            write(range(last_global_count, global_count))
            last_global_count = global_count
            break
        elif count >= chunksize:
            write(range(last_global_count, global_count))
            last_global_count = global_count
            data = []
            count = 0

def write_pandas(store, localpath, df, min_itemsize, 
                 min_item_padding=1.1, chunksize=500000, replace=True):
    """
    min_itemsize - dict of string columns, to length of string columns.  or can specify 'unknown'
    if unknown, we will compute it here
    min_item_padding, for computed min_item_sizes, we will multiply by padding to give
    ourselves some buffer
    """
    min_itemsize = copy.copy(min_itemsize)
    for k,v in min_itemsize.iteritems():
        if v == 'unknown':
            v = max(len(x) for x in df[k])
            v *= min_item_padding
            v = int(round(v))
            logger.debug("using minsize of %s for %s", v, k)
            min_itemsize[k] = v
    if replace:
        if localpath in store:
            store.remove(localpath)
    max_length = df.shape[0]
    starts = range(0, max_length, chunksize)
    ends = [x + chunksize for x in starts]
    ends[-1] = None # last slice writes all the way to the end of the df, no matter what
    for st, end in zip(starts, ends):
        subdata = df[st:end]
        logger.debug("writing rowstart %s rowend %s", st, end)
        store.append(localpath, subdata, min_itemsize=min_itemsize, 
                     chunksize=chunksize, data_columns=True)

class PandasHDFNode(Node, HDFDataSetMixin, HDFDataGroupMixin):
    def __init__(self, context, localpath="/"):
        super(PandasHDFNode, self).__init__(context)
        self.localpath = localpath
        self.store = get_pandas_hdf5(self.absolute_file_path)
        # this will either point to a hdf group, or an hdf table... maybe this is bad idea
        # to do this all in one class but for now...
        if self.store.keys() == ['__data__']:
            self.localpath = "/__data__"
            self.is_group = False
        elif self.localpath in self.store.keys():
            self.is_group = False
        else:
            self.is_group = True
            
    def keys(self):
        if not self.is_group:
            return ArrayManagementException, 'This node is not a group'
        keys = self.store.keys()
        keys = [x for x in keys if x.startswith(self.localpath) and x!= self.localpath]
        keys = [dirsplit(x, self.localpath)[0] for x in keys]
        return keys

    def get_node(self, key):
        if not self.is_group:
            return ArrayManagementException, 'This node is not a group'
        new_local_path = posixpath.join(self.localpath, key)
        return PandasHDFNode(self.context, localpath=new_local_path)

    def put(self, key, value, format='fixed', append=False):
        new_local_path = posixpath.join(self.localpath, key)
        if append:
            format = 'table'
            replace = False
        else:
            replace = True
        if format == 'table':
            write_pandas(self.store, new_localpath_path, value, 
                         self.min_itemsize, 
                         min_item_padding=self.min_item_padding,
                         chunksize=500000, 
                         replace=replace)
        else:
            self.store.put(new_local_path, value)
        self.store.flush()

import types

class PandasCacheable(Node):
    def cache_path(self):
        cache_name = "cache_%s.hdf5" % self.key
        apath = self.absolute_file_path
        if isfile(apath):
            cachedir = join(dirname(apath), '.cache')
        else:
            cachedir = join(apath, '.cache')
        if not exists(cachedir):
            os.makedirs(cachedir)
        cache_path = join(cachedir, cache_name)
        return cache_path

    def __init__(self, context, get_data=None):
        super(PandasCacheable, self).__init__(context)
        self.store = None
        self.localpath = "/" + posixpath.basename(context.urlpath)
        if get_data:
            self.get_data = types.MethodType(get_data, self)

    def _get_store(self):
        if not self.store:
            cache_path = self.cache_path()
            self.store = get_pandas_hdf5(cache_path)
        return self.store
        
class PandasCacheableTable(PandasCacheable):
    min_item_padding = 1.1
    min_itemsize = {}
    def load_data(self, force=False):
        store = self._get_store()
        if not force and self.localpath in store.keys():
            return
        data = self.get_data()
        logger.debug("GOT DATA with shape %s for %s, writing to pytables", data.shape, self.urlpath)
        write_pandas(self.store, self.localpath, data, 
                     self.min_itemsize, 
                     min_item_padding=self.min_item_padding,
                     chunksize=500000, 
                     replace=True)
        self.store.flush()
        return self

    def select(self, *args, **kwargs):
        self.load_data(force=kwargs.pop('force', None))
        return self.store.select(self.localpath, *args, **kwargs)

class PandasCacheableFixed(PandasCacheable):
    """extend this class to define custom pandas nodes
    """
    inmemory_cache = {}
    def load_data(self, force=False):
        store = self._get_store()
        if not force and self.localpath in self.inmemory_cache:
            return
        if not force and self.localpath in store:
            self.inmemory_cache[self.localpath] = self.store.get(self.localpath)
            return
        data = self.get_data()
        logger.debug("GOT DATA with shape %s, writing to pytables", data.shape)
        self.store.put(self.localpath, data)
        self.inmemory_cache[self.localpath] = data
        self.store.flush()
        return self

    def get(self, *args, **kwargs):
        self.load_data(force=kwargs.pop('force', None))
        return self.inmemory_cache[self.localpath]
