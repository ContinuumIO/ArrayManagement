import posixpath
from os.path import join, relpath
import pandas as pd

from arraymanagement import default_loader
from arraymanagement.nodes.hdfnodes import PandasCacheableTable


old_keys = default_loader.keys

old_get_node = default_loader.get_node

class MyCSVNode(PandasCacheableTable):
    is_group = False
    def _get_data(self):
        fname = join(self.basepath, self.relpath)
        data = pd.read_csv(fname, **self.config.get('csv_options'))
        data['values'] = data['values'] * 2
        return data

def get_node(urlpath, rpath, basepath, config):
    key = posixpath.basename(urlpath)
    if key == 'sample2':
        fname = "sample.csv"
        new_rpath = relpath(join(basepath, rpath, 'sample.csv'), basepath)
        return MyCSVNode(urlpath, new_rpath, basepath, config)        
    else:
        return old_get_node(urlpath, rpath, basepath, config)

def keys(urlpath, rpath, basepath, config):
    ks = old_keys(urlpath, rpath, basepath, config)
    ks.append('sample2')
    return ks
