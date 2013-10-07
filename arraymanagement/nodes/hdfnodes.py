import os
import copy
from os.path import basename, splitext, join, isfile, isdir, dirname
import posixpath
import math

import pandas as pd
import numpy as np

from ..exceptions import ArrayManagementException
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

def write_pandas_hdf_from_cursor(store, localpath, cursor, columns, min_itemsize, dt_fields,
                               min_item_padding=1.1, chunksize=500000, replace=True):
    """
    min_itemsize - dict of string columns, to length of string columns.  Cannot specify 'unknown'
    if unknown, we will compute it here
    min_item_padding, for computed min_item_sizes, we will multiply by padding to give
    ourselves some buffer
    """
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
            for dtfield in dt_fields:
                df[dtfield] = df[dtfield].astype('datetime64[ns]')
            logger.debug("writing rowend %s", global_count)
            store.append(localpath, df, min_itemsize=min_itemsize, 
                         chunksize=chunksize, data_columns=True)
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
    def __init__(self, urlpath, relpath, basepath, config, localpath="/"):
        super(PandasHDFNode, self).__init__(urlpath, relpath, basepath, config)
        self.localpath = localpath
        self.store = get_pandas_hdf5(join(basepath, relpath))

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
        keys = [posixpath.relpath(x, self.localpath) for x in keys]
        keys = [posixpath.basename(x) for x in keys]
        return keys

    def get_node(self, key):
        if not self.is_group:
            return ArrayManagementException, 'This node is not a group'
        new_local_path = join(self.localpath, key)
        return PandasHDFNode(self.urlpath, self.relpath, self.basepath, self.config,
                             localpath=new_local_path)

class PandasCacheable(Node):
    def __init__(self, urlpath, relpath, basepath, config):
        super(PandasCacheable, self).__init__(urlpath, relpath, basepath, config)
        self.store = None
        self.localpath = "/" + posixpath.basename(urlpath)
    def _get_store(self):
        if not self.store:
            cache_name = "cache_%s.hdf5" % posixpath.basename(self.urlpath)
            apath = join(self.basepath, self.relpath)
            if isfile(apath):
                cache_path = join(dirname(apath), cache_name)
            else:
                cache_path = join(apath, cache_name)
            self.store = get_pandas_hdf5(cache_path)
        return self.store
        
class PandasCacheableTable(PandasCacheable):
    min_item_padding = 1.1
    min_itemsize = {}
    def load_data(self):
        store = self._get_store()
        if not force and self.localpath in store.keys():
            return
        data = self._get_data()
        logger.debug("GOT DATA with shape %s, writing to pytables", data.shape)
        write_pandas(self.store, self.localpath, data, 
                     self.min_itemsize, 
                     min_item_padding=self.min_item_padding,
                     chunksize=500000, 
                     replace=True)
    def select(self, *args, **kwargs):
        self.load_data(force=kwargs.pop('force', None))
        return self.store.select(self.localpath, *args, **kwargs)

class PandasCacheableFixed(PandasCacheable):
    inmemory_cache = {}
    def load_data(self, force=False):
        store = self._get_store()
        if not force and self.localpath in self.inmemory_cache:
            return
        if not force and self.localpath in store:
            self.inmemory_cache[self.localpath] = self.store.get(self.localpath)
            return
        data = self._get_data()
        logger.debug("GOT DATA with shape %s, writing to pytables", data.shape)
        self.store.put(self.localpath, data)
        self.inmemory_cache[self.localpath] = self.store.get(self.localpath)
        return data

    def get(self, *args, **kwargs):
        self.load_data(force=kwargs.pop('force', None))
        return self.inmemory_cache[self.localpath]

        
    
    

