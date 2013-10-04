import posixpath
import os
from os.path import basename, splitext, join, dirname
import pandas as pd

class Node(object):
    def __init__(self, urlpath, relpath, basepath, config):
        """    
        urlpath : urlpath to this location
        relpath : file path to this location from relative to basepath
        basepath : file system path to the root of the tree
        config : instance of arraymanagement.config.NodeConfig
        """
        self.urlpath = urlpath
        self.relpath = relpath
        self.basepath = basepath
        self.config = config
        self.absolute_file_path = join(basepath, relpath)

class PandasCacheable(Node):
    def __init__(self, urlpath, relpath, basepath, config):
        super(PandasCacheable, self).__init__(urlpath, relpath, basepath, config)
        self.store = None
        self.localpath = "/" + posixpath.basename(urlpath)

    def _get_store(self):
        if not self.store:
            cache_name = self.config.get('array_cache')
            cache_path = join(dirname(join(self.basepath, self.relpath)), cache_name)
            self.store = pd.HDFStore(cache_path)
        return self.store

    def load_data(self):
        store = self._get_store()
        if self.localpath in store.keys():
            return
        data = self._get_data()
        self.store.put(self.localpath, data, table=True)

    def select(self, *args, **kwargs):
        self.load_data()
        return self.store.select(self.localpath, *args, **kwargs)

        
