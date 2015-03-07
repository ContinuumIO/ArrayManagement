import os
from os.path import basename, splitext, join, isdir, relpath
import posixpath
import pandas as pd

from .exceptions import ArrayManagementException
import fnmatch
from .nodes import dirnodes
import sys

def keys(context, overrides={}):
    fnames = os.listdir(context.absolute_file_path)
    fnames = [x for x in fnames if not (x.startswith('cache') and x.endswith('hdf5'))]
    fnames = [x for x in fnames if not x.startswith('.')]
    ks = []
    loaders = context.config.get('loaders')
    names = set()
    for pattern in loaders:
        matches = fnmatch.filter(fnames, pattern)
        matches = [splitext(match)[0] for match in matches]
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

def get_node(key, context, overrides={}):
    """
    urlpath : to the resource you are seeking
    rpath : path to this directory
    basepath : basepath of directory tree
    """
    urlpath = context.joinurl(key)
    abspath = context.absolute_file_path
    if key in overrides:
        return dispatch(overrides[key], context.clone(urlpath=urlpath))
    files = os.listdir(abspath)
    if key in files:
        fname = key
    else:
        files = [x for x in files if splitext(x)[0] == key]

        if len (files) > 1:
            raise ArrayManagementException('multile files matching {}: {}'.format(key, str(files)))

        if len (files) == 0:
            raise ArrayManagementException('No files matching {}'.format(key))
        fname = files[0]
    new_abspath = context.joinpath(fname)
    new_rpath = context.rpath(new_abspath)

    if isdir(new_abspath):
        new_config = context.config.clone_and_update(new_rpath)
        newcontext = context.clone(relpath=new_rpath, config=new_config, urlpath=urlpath)
        return dirnodes.DirectoryNode(newcontext, default_mod=sys.modules[__name__])

    newcontext = context.clone(relpath=new_rpath, urlpath=urlpath)
    loaders = context.config.get('loaders')
    pattern_priority = context.config.get('pattern_priority')
    for pattern in pattern_priority and loaders:
        if fnmatch.fnmatch(fname, pattern):
            return dispatch(loaders[pattern], newcontext)
    for pattern in loaders:
        if fnmatch.fnmatch(fname, pattern):
            return dispatch(loaders[pattern], newcontext)
    return None
