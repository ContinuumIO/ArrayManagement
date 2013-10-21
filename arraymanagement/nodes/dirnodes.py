from os.path import basename, splitext, join, exists
import posixpath
from . import Node, NodeContext
from .. import pathutils

import imp
import logging
logger = logging.getLogger(__name__)

class DirectoryNode(Node):
    is_group = True
    def __init__(self, context, mod=None):
        super(DirectoryNode, self).__init__(context)
        loadpath = self.joinpath("load.py")
        if exists(loadpath):
            directories = pathutils.dirsplit(self.relpath, self.basepath)
            name = "_".join(directories)
            name += "_load"
            self.mod = imp.load_source(name, loadpath)
        else:
            self.mod = mod
    
    def overrides(self):
        if hasattr(self.mod, 'overrides'):
            return self.mod.overrides
        else:
            return {}

    def keys(self):
        overrides = {}
        return self.mod.keys(context, overrides=overrides)
    
    def get_node(self, key):
        overrides = {}
        urlpath = self.joinurl(key)
        logger.debug("retrieving url %s", urlpath)
        return self.mod.get_node(key, self.context, overrides=overrides)


    
        
