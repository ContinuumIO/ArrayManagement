#import databag
import os
from os.path import join, dirname, isdir, relpath, exists, abspath

from .config import NodeConfig

from . import default_loader
from . import pathutils

from .nodes.dirnodes import DirectoryNode
from .nodes import Node, NodeContext
import sys
import os
import shutil
from . import clear_mem_cache
from .logger import log
from imp import reload

class ArrayClient(Node):
    #should modify this to inherit from DirectorNode
    is_group = True
    def __init__(self, basepath, configname="datalib.config", group_write=True, localdatapath=None):
        self.root = abspath(basepath)
        self.debug = True
        if self.root not in sys.path:
            sys.path.append(self.root)

        if localdatapath:
            sys.path.insert(0,localdatapath)

        try:
            self.raw_config = __import__(configname, fromlist=[''])
        except ImportError:
            default_configname = "arraymanagement.defaultconfig"
            logger.error("could not load config %s", configname)
            logger.error("iporting default config %s instead", default_configname)
            self.raw_config = __import__(default_configname, fromlist=[''])

        self.config = self.get_config()

        try:
            cache_dir = self.config.config['cache_dir']
            if '~' in cache_dir:
                cache_dir = os.path.expanduser(cache_dir)
        except KeyError:
            cache_dir = self.root

        context = NodeContext("/", self.root, self,cache_dir=cache_dir)
        if group_write:
            os.umask(2)
        super(ArrayClient, self).__init__(context)

    def get_config(self, urlpath="/", parent_config=None):
        if parent_config is None:
            parent_config = self.raw_config.global_config
        if self.debug:
            if self.raw_config.local_config.get(urlpath) \
                    and self.raw_config.local_config.get(urlpath).get('__module__'):
                reload(self.raw_config.local_config.get(urlpath).get('__module__'))
            reload(self.raw_config)
        config = NodeConfig(urlpath,
                            parent_config,
                            self.raw_config.local_config.get(urlpath, {}))
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

    def clear_mem_cache(self):
        clear_mem_cache()

    def set_logging(self,external_log):
        '''
        setup external logger
        '''

        for l in external_log.handlers:
            log.addHandler(l)

    def clear_disk_cache(self, url=None):
        if url is None:
            path = self.root
        else:
            names = pathutils.urlsplit(url, "/")
            path = join(self.root, *names)
        for dirpath, dirnames, filenames in os.walk(path):
            if os.path.split(dirpath)[-1] == ".cache":
                shutil.rmtree(dirpath)




