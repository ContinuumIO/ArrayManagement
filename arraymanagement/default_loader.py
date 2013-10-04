import os
from os.path import basename, splitext, join, isdir, relpath
import posixpath
import pandas as pd

from .exceptions import ArrayManagementException
from nodes import csvnodes
from nodes import hdfnodes
from nodes import dirnodes
import sys

def keys(urlpath, rpath, basepath, config):
    fnames = os.listdir(join(basepath, rpath))
    ks = [basename(x) for x in fnames]
    
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
    if extension in config.get('csv_exts'):
        return csvnodes.PandasCSVNode(urlpath, new_rpath, basepath, config)
    elif extension in config.get('hdf5_exts'):
        return hdfnodes.PandasHDFNode(urlpath, new_rpath, basepath, config)
    else:
        return None
