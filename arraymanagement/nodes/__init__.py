import posixpath
import os
from os.path import basename, splitext, join, dirname, isdir, isfile
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
        self.key = posixpath.basename(urlpath)

    def __getitem__(self, k):
        return self.get_node(k)
    def __repr__(self):
        return ":".join([self.__class__.__name__, self.urlpath, self.relpath])

    def joinpath(self, path):
        return posixpath.normpath(posixpath.join(self.urlpath, path))

    @property
    def c(self):
        return self.config.client
