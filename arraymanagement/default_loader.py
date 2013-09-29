import os
from os.path import basename, splitext, join
import posixpath

def keys(urlpath, relpath, basepath, config):
    """return keys for a given location
    urlpath : urlpath to this location
    relpath : file path to this location from relative to basepath
    basepath : file system path to the root of the tree
    config : instance of arraymanagement.config.NodeConfig
    ----
    returns : list of dicts, where dicts specify information about the different types
    of data that live in this part of the hierarchy
    """
    fnames = os.listdir(path)
    ks = [{'name' : basename(x), 'extension' : splitext(x), 'filename' : x} for x in fnames]
    for k in ks:
        if k['extension'] in config.get('csv_exts'):
            k['type'] = 'csv'
            k['csv_reader'] = config.get('csv_reader')
            k['csv_options'] = config.get('csv_options')
            k['urlpath'] = posixpath.join(urlpath, k['name'])
            k['abs_file_path'] = join(basepath, relpath, k['filename'])

        elif k['extension'] in config.get('hdf5_exts') and \
                k['filename'] != config.get('array_cache'):
            k['type'] = 'hdf5'
            k['hdf5_type'] = config.get('hdf5_type')
            k['urlpath'] = posixpath.join(urlpath, k['name'])
            k['abs_file_path'] = join(basepath, relpath, k['filename'])

        elif k['filename'] == config.get('array_cache'):
            pass
    
