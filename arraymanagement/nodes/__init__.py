import posixpath
import os
from os.path import basename, splitext, join, dirname, isdir, isfile
import pandas as pd

class NodeContext(object):
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
        self.key = posixpath.basename(urlpath)

    def joinurl(self, path):
        return posixpath.normpath(posixpath.join(self.urlpath, path))

    def joinpath(self, path):
        return os.path.normpath(os.path.join(self.absolute_file_path, path))

    def rpath(self, path):
        return os.path.relpath(path, self.basepath)

    @property
    def c(self):
        return self.config.client

    def clone(self, **kwargs):
        args = {'urlpath' : self.urlpath, 
                'relpath' : self.relpath, 
                'basepath' : self.basepath, 
                'config' : self.config}
        args.update(kwargs)
        return self.__class__(args['urlpath'], args['relpath'], 
                              args['basepath'], args['config'])
        
    
class Node(object):
    def __init__(self, context):
        self.urlpath = context.urlpath
        self.relpath = context.relpath
        self.basepath = context.basepath
        self.config = context.config
        self.absolute_file_path = context.absolute_file_path
        self.key = context.key
        self.context = context

    def __getitem__(self, k):
        return self.get_node(k)
    
    def __repr__(self):
        info = ["type: %s" % self.__class__.__name__,
                "urlpath: %s" % self.urlpath,
                "filepath: %s" % self.relpath]
        if hasattr(self, 'keys') and self.is_group:
            info.append('keys: %s' % str(self.keys()))
        return "\n".join(info)

    @property
    def c(self):
        return self.context.c

    def joinurl(self, path):
        return self.context.joinurl(path)

    def joinpath(self, path):
        return self.context.joinpath(path)

    def rpath(self, path):
        return self.context.rpath(path)
