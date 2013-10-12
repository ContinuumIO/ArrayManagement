import os
from os.path import basename, splitext, join, isdir, relpath
import posixpath
import pandas as pd

from .exceptions import ArrayManagementException
import fnmatch
from nodes import dirnodes
import sys

def keys(urlpath, rpath, basepath, config):
    fnames = os.listdir(join(basepath, rpath))
    fnames = [x for x in fnames if not (x.startswith('cache') and x.endswith('hdf5'))]
    ks = []
    loaders = config.get('loaders')
    names = set()
    for pattern in loaders:
        matches = fnmatch.filter(fnames, pattern)
        matches = [splitext(match)[0] for match in matches]
        names.update(matches)
    for fname in fnames:
        if isdir(join(basepath, rpath, fname)):
            names.add(fname)
    return list(names)

def get_node(urlpath, rpath, basepath, config):
    """
    urlpath : to the resource you are seeking
    rpath : path to this directory
    basepath : basepath of directory tree
    """
    abspath = join(basepath, rpath)
    key = posixpath.basename(urlpath)
    files = os.listdir(abspath)
    files = [x for x in files if splitext(x)[0] == key]
    if len (files) > 1:
        raise ArrayManagementException, 'multile files matching %s: %s' % (key, str(files))
    fname = files[0]
    new_abspath = join(abspath, fname)
    new_rpath = relpath(new_abspath, basepath)
    if isdir(new_abspath):
        new_config = config.clone_and_update(new_rpath)
        return dirnodes.DirectoryNode(urlpath, new_rpath, basepath, new_config, 
                                      mod=sys.modules[__name__])
    prefix, extension = splitext(new_abspath)
    loaders = config.get('loaders')
    pattern_priority = config.get('pattern_priority')
    for pattern in pattern_priority and loaders:
        if fnmatch.fnmatch(fname, pattern):
            return loaders[pattern](urlpath, new_rpath, basepath, config)
    for pattern in loaders:
        if fnmatch.fnmatch(fname, pattern):
            return loaders[pattern](urlpath, new_rpath, basepath, config)
    return None
