import databag
import os
from os.path import join, dirname, isdir, relpath
from config import NodeConfig
import pathutils
from nodes.dirnodes import DirectoryNode
import default_loader
class ArrayClient(object):
    def __init__(self, path):
        self.root = path
        self.config = NodeConfig.from_paths(self.root, self.root)

    def get_node(self, urlpath):
        names = pathutils.urlsplit(urlpath, "/")
        basepath = self.root
        rpath = relpath(basepath, basepath)
        basenode = DirectoryNode("/", rpath, basepath, self.config, mod=default_loader)
        node = basenode
        for n in names:
            node = node.get_node(n)
        return node
            
            
            
        

