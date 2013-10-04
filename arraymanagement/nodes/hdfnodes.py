import os
from os.path import basename, splitext, join
import posixpath
import pandas as pd

from ..exceptions import ArrayManagementException
from . import Node

class HDFDataGroupMixin(object):
    def put(self, key, *args, **kwargs):
        new_local_path = join(self.localpath, key)
        return self.store.put(new_local_path, *args, **kwargs)

class HDFDataSetMixin(object):
    def select(self, *args, **kwargs):
        return self.store.select(self.localpath, *args, **kwargs)
    
    def append(self, *args, **kwargs):
        return self.store.append(self.localpath, *args, **kwargs)
    
class PandasHDFNode(Node, HDFDataSetMixin, HDFDataGroupMixin):
    def __init__(self, urlpath, relpath, basepath, config, localpath="/"):
        super(PandasHDFNode, self).__init__(urlpath, relpath, basepath, config)
        self.localpath = localpath
        self.store = pd.HDFStore(join(basepath, relpath))

        # this will either point to a hdf group, or an hdf table... maybe this is bad idea
        # to do this all in one class but for now...

        if self.store.keys() == ['__data__']:
            self.localpath = "/__data__"
            self.is_group = False
        elif self.localpath in self.store.keys():
            self.is_group = False
        else:
            self.is_group = True
            
    def keys(self):
        if not self.is_group:
            return ArrayManagementException, 'This node is not a group'
        keys = self.store.keys()
        keys = [x for x in keys if x.startswith(self.localpath) and x!= self.localpath]
        keys = [posixpath.relpath(x, self.localpath) for x in keys]
        keys = [posixpath.basename(x) for x in keys]
        return keys

    def get_node(self, key):
        if not self.is_group:
            return ArrayManagementException, 'This node is not a group'
        new_local_path = join(self.localpath, key)
        return PandasHDFNode(self.urlpath, self.relpath, self.basepath, self.config,
                             localpath=new_local_path)
        

    

    
    

