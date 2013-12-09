import os
from os.path import basename, splitext, join, isdir, relpath
import posixpath
import pandas as pd
from ..exceptions import ArrayManagementException
import fnmatch
import sys

from . import Node
from .. import pathutils

import logging
logger = logging.getLogger(__name__)

def keys(context, overrides={}):
    fnames = os.listdir(context.absolute_file_path)
    fnames = [x for x in fnames if not (x.startswith('cache') and x.endswith('hdf5'))]
    fnames = [x for x in fnames if not x.startswith('.')]
    ks = []
    loaders = context.config.get('loaders')
    names = set()
    for pattern in loaders:
        matches = fnmatch.filter(fnames, pattern)
        names.update(matches)
    for fname in fnames:
        if isdir(context.joinpath(fname)):
            names.add(fname)
    names.update(overrides.keys())
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
            raise ArrayManagementException, 'multile files matching %s: %s' % (key, str(files))
        if len(files) == 1:
            #hack
            fname = files[0]
            key = fname
            urlpath = context.joinurl(key)
        else:
            fname = None

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
    if isdir(abspath):
        return DirectoryNode(context)
    return None

class DirectoryNode(Node):
    is_group = True
    def keys(self):
        return keys(self.context)

    def get_node(self, key):
        urlpath = self.joinurl(key)
        logger.debug("retrieving url %s", urlpath)
        return get_node(key, self.context)
