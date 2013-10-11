#import databag
import os
from os.path import join, dirname, isdir, relpath, exists, abspath
from config import NodeConfig
import pathutils
from nodes.dirnodes import DirectoryNode
from nodes import Node
import default_loader
import sys

class ArrayClient(Node):
    #should modify this to inherit from DirectorNode
    def __init__(self, path):
        self.root = abspath(path)
        self.config = NodeConfig.from_paths(self.root, self.root, self)
        if self.root not in sys.path:
            sys.path.append(self.root)
        super(ArrayClient, self).__init__("/", ".", self.root, self.config)

    def get_node(self, urlpath):
        if not urlpath.startswith("/"):
            urlpath = "/" + urlpath
        names = pathutils.urlsplit(urlpath, "/")
        basepath = self.root
        rpath = relpath(basepath, basepath)
        basenode = DirectoryNode("/", rpath, basepath, self.config, mod=default_loader)
        node = basenode
        for n in names:
            node = node.get_node(n)
        return node

    def keys(self):
        return self.get_node('/').keys()
            
        

    
