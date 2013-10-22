from os.path import basename, splitext, join, exists
import posixpath
from . import Node, NodeContext
from .. import pathutils

import imp
import logging
logger = logging.getLogger(__name__)

class DirectoryNode(Node):
    is_group = True
    def __init__(self, context, default_mod=None):
        super(DirectoryNode, self).__init__(context)
        loadpath = self.joinpath("load.py")
        self.default_mod = default_mod
        if exists(loadpath):
            directories = pathutils.dirsplit(self.relpath, self.basepath)
            name = "_".join(directories)
            name += "_load"
            self.mod = imp.load_source(name, loadpath)
        else:
            self.mod = default_mod
    
    def overrides(self):
        if hasattr(self.mod, 'overrides'):
            return self.mod.overrides
        else:
            return {}

    def keys(self):
        overrides = {}
        if hasattr(self.mod, 'keys'):
            return self.mod.keys(self.context, overrides=overrides)
        else:
            return self.default_mod.keys(self.context, overrides=overrides)
    
    def get_node(self, key):
        overrides = {}
        urlpath = self.joinurl(key)
        logger.debug("retrieving url %s", urlpath)
        if hasattr(self.mod, 'get_node'):
            return self.mod.get_node(key, self.context, overrides=overrides)
        else:
            return self.default_mod.get_node(key, self.context, overrides=overrides)


    
        
