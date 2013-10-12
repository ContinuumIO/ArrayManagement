from os.path import basename, splitext, join, exists
import posixpath
from . import Node
from .. import pathutils

import imp
import logging
logger = logging.getLogger(__name__)

class DirectoryNode(Node):
    is_group = True
    def __init__(self, urlpath, relpath, basepath, config, mod=None):
        super(DirectoryNode, self).__init__(urlpath, relpath, basepath, config)
        loadpath = join(basepath, relpath, "load.py")
        if exists(loadpath):
            directories = pathutils.dirsplit(relpath, basepath)
            name = "_".join(directories)
            name += "_load"
            self.mod = imp.load_source(name, loadpath)
        else:
            self.mod = mod

    def keys(self):
        return self.mod.keys(self.urlpath, self.relpath, self.basepath, self.config)
    
    def get_node(self, key):
        urlpath = posixpath.join(self.urlpath, key)
        logger.debug("retrieving url %s", urlpath)
        return self.mod.get_node(urlpath, self.relpath, self.basepath, self.config)

    
        
