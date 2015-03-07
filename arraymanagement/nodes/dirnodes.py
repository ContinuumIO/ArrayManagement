import os
from os.path import basename, splitext, join, isdir, relpath
import posixpath
import pandas as pd
from ..exceptions import ArrayManagementException
import fnmatch
import sys
from .hdfnodes import get_pandas_hdf5, write_pandas
from . import Node
from .. import pathutils

import logging
logger = logging.getLogger(__name__)

def keys(context):
    fnames = os.listdir(context.absolute_file_path)
    fnames = [x for x in fnames if not (x.startswith('cache') and x.endswith('hdf5'))]
    fnames = [x for x in fnames if not x.startswith('.')]
    urlpaths = [context.joinurl(key) for key in fnames]
    ks = []
    loaders = context.config.get('loaders')
    names = set()
    for pattern in loaders:
        if "*" in pattern:
            matches = fnmatch.filter(urlpaths, pattern)
            names.update([posixpath.basename(x) for x in matches])
        else:
            names.add(posixpath.basename(pattern))
    for fname in fnames:
        if isdir(context.joinpath(fname)):
            names.add(fname)
    return list(names) 

def dispatch(loader, context):
    if isinstance(loader, (tuple, list)):
        return loader[0](context, **loader[1])
    else:
        return loader(context)

def get_node(key, context):
    """
    urlpath : to the resource you are seeking
    rpath : path to this directory
    basepath : basepath of directory tree
    """
    urlpath = context.joinurl(key)
    abspath = context.absolute_file_path
    loaders = context.config.get('loaders')
    #match up filename with key
    files = os.listdir(abspath)
    if key in files:
        fname = key
    else:
        #DEPRECATE THIS..
        files = [x for x in files if splitext(x)[0] == key]
        if len (files) > 1:
            raise ArrayManagementException('multile files matching {}: {}'.format(key, str(files)))
        if len(files) == 1:
            #hack
            fname = files[0]
            key = fname
            urlpath = context.joinurl(key)
        else:
            fname = None

    abspath = None
    #construct new context
    if fname is None:
        context = context.clone(urlpath=urlpath)
    else:
        abspath = context.joinpath(fname)
        context = context.clone(urlpath=urlpath, absolute_file_path=abspath)
    #dispatch directory nodes
    if key in loaders:
        return dispatch(loaders[key], context)
    for pattern in loaders:
        if fnmatch.fnmatch(urlpath, pattern):
            return dispatch(loaders[pattern], context)
    if abspath and isdir(abspath):
        return DirectoryNode(context)
    return None

class DirectoryNode(Node):
    is_group = True
    def keys(self):
        if self.context.config.get('keys'):
            keyfunc = self.context.config.get('keys')
            return keyfunc(self.context)
        else:
            return keys(self.context)

    def get_node(self, key):
        urlpath = self.joinurl(key)
        logger.debug("retrieving url %s", urlpath)
        return get_node(key, self.context)
    
    def put(self, key, data, format='fixed', append=False, min_itemsize={}):
        file_path = join(self.absolute_file_path, key + ".hdf5")
        store = get_pandas_hdf5(file_path)
        if append:
            format = 'table'
            replace = False
        else:
            replace = True
        if format == 'table':
            write_pandas(store, "__data__", data, 
                         min_itemsize, 
                         chunksize=500000, 
                         replace=replace)
        else:
            store.put("__data__", data)
        store.flush()

        
