import json
import os
from os.path import join, dirname, exists
import copy

import databag

from pathutils import recursive_config_load, get_config

class NodeConfig(object):
    is_dataset = False
    csv_exts = ['.csv', '.CSV']
    json_exts = ['.json', '.JSON']
    hdf5_exts = ['.hdf5', '.h5']
    sql_exts = ['.sql', '.SQL']
    hdf5_type = 'pandas'
    json_type = 'pandas'
    csv_reader = 'pandas'
    array_cache = '__init__.hdf5'
    csv_options = {}
    def __init__(self, path, basepath, config, client):
        self.config = config
        self.path = path
        self.basepath = basepath
        self.md = databag.DataBag(fpath=join(basepath, "__md.db"))
        self.client = client

    @classmethod
    def from_paths(cls, path, basepath, client):
        config = recursive_config_load(path, basepath)
        return cls(path, basepath, config, client)

    def get(self, key):
        return self.config.get(key, getattr(self, key, None))
    
    def clone_and_update(self, relpath):
        """clones this node, and updates from a config in path
        """
        new_config = copy.copy(self.config)
        new_config.update(get_config(join(self.basepath, relpath), self.basepath))
        return NodeConfig(relpath, self.basepath, new_config, self.client)
        
    
        
        
