import json
import os
from os.path import join, dirname, exists
import posixpath
import copy
import collections
from .nodes import csvnodes
from .nodes import hdfnodes
from .nodes import dirnodes
from .nodes import sql

#import databag

from .pathutils import recursive_config_load, get_config
import collections

def ordered_dict_merge(first, second):
    """merges 2 ordered dictionaries, overlaying the second
    on the first, but preserving the keys of the second
    """
    # There is probably a better way to do this...
    output = collections.OrderedDict()
    for k in first:
        output[k] = first[k]
    for k in second:
        output[k] = second[k]
    output2 = collections.OrderedDict()
    for k in second:
        output2[k] = output[k]
    for k in first:
        output2[k] = output[k]
    return output2

def config_dict_update(old_config, new_config):
    """takes one config dictionary, and updates it with another.
    also updates all dicts inside new_config with the same dicts in old_config
    """
    new = {}

    keys = set(list(old_config.keys()) + list(new_config.keys()))

    for k in keys:
        if k in old_config and k in new_config and \
                isinstance(old_config[k], dict) and \
                isinstance(new_config[k], dict):
            new[k] = ordered_dict_merge(old_config[k], new_config[k])
            continue
        if k in old_config:
            new[k]  = old_config[k]
        if k in new_config:
            new[k] = new_config[k]
    return new

# base_config = dict(
#     is_dataset = False,
#     csv_options = {},
#     table_type_overrides = {},
#     datetime_type = 'datetime64[ns]',
#     loaders = {
#         '*.csv' : csvnodes.PandasCSVNode,
#         '*.CSV' : csvnodes.PandasCSVNode,
#         '*.hdf5' : hdfnodes.PandasHDFNode,
#         '*.h5' : hdfnodes.PandasHDFNode,
#         '*.sql' : sql.SimpleQueryTable,
#         },
#     pattern_priority = []
#     )

class NodeConfig(object):
    def __init__(self, url, parent_config, local_config):
        # local_loaders = local_config.get('loaders', {})
        # new_loaders = collections.OrderedDict()
        # for key, loader in local_loaders.iteritems():
        #     new_key = posixpath.join(url, key)
        #     new_loaders[new_key] = loader
        # local_config['loaders'] = new_loaders
        self.config = config_dict_update(parent_config, local_config)

    def get(self, key, default=None):
        return self.config.get(key, getattr(self, key, default))


