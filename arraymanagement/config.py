import json
import os
from os.path import join, dirname, exists

import databag

from pathutils import recursive_config_load

class NodeConfig(object):
    is_dataset = False
    csv_exts = ['csv', 'CSV']
    json_exts = ['json', 'JSON']
    hdf5_exts = ['hdf5', 'h5']
    hdf5_type = 'pandas'
    json_type = 'pandas'
    csv_reader = 'pandas'
    array_cache = '__init__.hdf5'

    def __init__(self, path, config):
        self.config = config
        self.path = path
        self.md = databag.DataBag(join(path, "__md.db"))

    @classmethod
    def from_paths(cls, path, basepath):
        config = recursive_js_load(path, basepath)
        return cls(path, existing_config=config)

    def get(self, key):
        return self.config.get(key)

        
        
