import json
import os
from os.path import join, dirname, exists

class NodeConfig(object):
    csv_exts = ['csv', 'CSV']
    json_exts = ['json', 'JSON']
    hdf5_exts = ['hdf5', 'h5']
    hdf5_type = 'pandas'
    json_type = 'pandas'
    csv_reader = 'pandas'
    array_cache = '__init__.hdf5'
    def __init__(self, path, existing_config={}):
        cfile = join(path, 'config.json')
        if exists(cfile):
            
        
        
