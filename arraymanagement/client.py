#import databag
import os
from os.path import join, dirname, isdir, relpath, exists, abspath
from config import NodeConfig
import pathutils
from nodes.dirnodes import DirectoryNode
from nodes import Node, NodeContext
import default_loader
import sys
import os

class ArrayClient(Node):
    #should modify this to inherit from DirectorNode
    is_group = True
    def __init__(self, path, configname="datalib.config", group_write=True):
        self.root = abspath(path)
        if self.root not in sys.path:
            sys.path.append(self.root)
        self.raw_config = __import__(configname, fromlist=[''])
        self.config = self.get_config()
        context = NodeContext("/", self.root, self)
        if group_write:
            os.umask(2)
        super(ArrayClient, self).__init__(context)

    def get_config(self, urlpath="/"):
        config = NodeConfig(urlpath,
                            self.raw_config.global_config, 
                            self.raw_config.local_config)
        return config
        
        
    def get_node(self, urlpath):
        if not urlpath.startswith("/"):
            urlpath = "/" + urlpath
        names = pathutils.urlsplit(urlpath, "/")
        basepath = self.root
        rpath = relpath(basepath, basepath)
        basenode = DirectoryNode(self.context)
        node = basenode
        for n in names:
            node = node.get_node(n)
        return node

    def keys(self):
        return self.get_node('/').keys()
            
        

    
