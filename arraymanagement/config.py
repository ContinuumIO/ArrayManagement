import json
import os
from os.path import join, dirname, exists
import posixpath
import copy
from nodes import csvnodes
from nodes import hdfnodes
from nodes import dirnodes
from nodes import sql

#import databag

from pathutils import recursive_config_load, get_config

def config_dict_update(old_config, new_config):
    """takes one config dictionary, and updates it with another.
    also updates all dicts inside new_config with the same dicts in old_config
    """
    new = {}
    keys = set(old_config.keys() + new_config.keys())
    for k in keys:
        if k in old_config and k in new_config and \
                isinstance(old_config[k], dict) and \
                isinstance(new_config[k], dict):
            new[k] = old_config[k]
            new[k].update(new_config[k])
            continue
        if k in old_config:
            new[k]  = old_config[k]
        if k in new_config:
            new[k] = new_config[k]
    return new

base_config = dict(
    is_dataset = False,
    csv_options = {},
    table_type_overrides = {},
    datetime_type = 'datetime64[ns]',
    loaders = {
        '*.csv' : csvnodes.PandasCSVNode,
        '*.CSV' : csvnodes.PandasCSVNode,
        '*.hdf5' : hdfnodes.PandasHDFNode,
        '*.h5' : hdfnodes.PandasHDFNode,
        '*.sql' : sql.SimpleQueryTable,
        },
    pattern_priority = []
    )            
class NodeConfig(object):
    def __init__(self, path, basepath, config, client):
        self.config = config
        self.path = path
        self.basepath = basepath
        #self.md = databag.DataBag(fpath=join(basepath, "__md.db"))
        self.client = client
        
    @classmethod
    def from_paths(cls, path, basepath, client):
        config = recursive_config_load(path, basepath)
        config = config_dict_update(base_config, config)
        return cls(path, basepath, config, client)

    def get(self, key):
        return self.config.get(key, getattr(self, key, None))

    def clone_and_update(self, relpath):
        """clones this node, and updates from a config in path
        """
        old_config = copy.copy(self.config)
        new_config = get_config(join(self.basepath, relpath), self.basepath)
        new = config_dict_update(old_config, new_config)
        return NodeConfig(relpath, self.basepath, new, self.client)
        
    
        
        
