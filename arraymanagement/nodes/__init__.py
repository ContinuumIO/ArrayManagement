import posixpath
import os
from os.path import basename, splitext, join, dirname, isdir, isfile, relpath
import pandas as pd
import logging

from ..exceptions import ArrayManagementException

logger = logging.getLogger(__name__)

class NodeContext(object):
    def __init__(self, urlpath, absolute_file_path, client,cache_dir,
                 parent_config=None):

        """    
        urlpath : urlpath to this location
        relpath : file path to this location from relative to basepath
        basepath : file system path to the root of the tree
        config : instance of arraymanagement.config.NodeConfig
        """
        self.urlpath = urlpath
        self.absolute_file_path = absolute_file_path
        self.client = client
        self.key = posixpath.basename(urlpath)
        self.cache_dir = cache_dir or '/'
        self.config = self.client.get_config(urlpath=self.urlpath, 
                                             parent_config=parent_config)
                
    def __getitem__(self, k):
        try:
            return self.c.get_node(self.joinurl(k))
        except ArrayManagementException as e:
            logger.exception(e)
            return None
    @property
    def basepath(self):
        return self.client.root

    @property 
    def relpath(self):
        return relpath(self.absolute_file_path, self.basepath)

    def joinurl(self, path):
        return posixpath.normpath(posixpath.join(self.urlpath, path))

    def joinpath(self, path):
        return os.path.normpath(os.path.join(self.absolute_file_path, path))

    def rpath(self, path):
        return os.path.relpath(path, self.basepath)

    @property
    def c(self):
        return self.client

    def clone(self, **kwargs):
        try:
            cache_dir = self.config.config['cache_dir']

            if '~' in cache_dir:
                cache_dir = os.path.expanduser(cache_dir)

        except KeyError:
            cache_dir = self.absolute_file_path

        args = {'urlpath' : self.urlpath, 
                'absolute_file_path' : self.absolute_file_path,
                'client' : self.client,
                'parent_config' : self.config.config,
                'cache_dir': cache_dir
                }
        args.update(kwargs)
        return self.__class__(args['urlpath'], 
                              args['absolute_file_path'],
                              args['client'],
                              args['cache_dir'],
                              parent_config=args['parent_config'],
                              )
        
display_limit=100
class Node(object):
    config_fields = []

    def __init__(self, context, **kwargs):
        self.urlpath = context.urlpath
        self.relpath = context.relpath
        self.basepath = context.basepath
        self.config = context.config
        self.absolute_file_path = context.absolute_file_path
        self.cache_dir = context.cache_dir
        self.key = context.key
        self.context = context
        for field in self.config_fields:
            if field in kwargs:
                setattr(self, field, kwargs.pop(field))
            elif self.config.get(field) is not None:
                setattr(self, field, self.config.get(field))
            else:
                setattr(self, field, None)

    def __getitem__(self, k):
        return self.context.__getitem__(k)

    def repr_data(self):
        info = ["type: %s" % self.__class__.__name__,
                "urlpath: %s" % self.urlpath,
                "filepath: %s" % self.relpath]
        if hasattr(self, 'keys') and self.is_group:
            keys = self.keys()
            if len(keys) >= display_limit:
                info.append('%s keys: %s ...' % (len(keys), ",".join(keys[:display_limit])))
            else:
                info.append('%s keys: %s ' % (len(keys), ",".join(keys)))
        return info

    def __repr__(self):
        info = self.repr_data()
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
    
    def descendant_urls(self, ignore_groups=False):
        """return urls of descendants. see descendants
        """
        nodes = self.descendants(ignore_groups=ignore_groups)
        return [x.urlpath for x in nodes]

    def descendants(self, ignore_groups=False):
        """return descendants
        if ignore_groups is true, only returns descendant arrays, not 
        descendant directories
        """
        descendants = []
        if not self.is_group:
            return descendants
        else:
            keys = self.keys()
            for k in keys:
                node = self[k]
                descendants.append(node)
                descendants.extend(node.descendants())
        if ignore_groups:
            descendants = [x for x in descendants if not x.is_group]
        return descendants
        
def store_select(pandas_store, key, where=None, **kwargs):
    
    if  "0.12" not in pd.__version__ and isinstance(where, list):
        where = [parse_back_compat(x) for x in where]
    # we used to accidentally pass series into start/stop
    if 'start' in kwargs:
        kwargs['start'] = int(kwargs['start'])
    if 'stop' in kwargs:
        kwargs['stop'] = int(kwargs['stop'])
    return pandas_store.select(key, where=where, **kwargs)

"""From pandas 
"""
def parse_back_compat(w, op=None, value=None):
    import warnings
    from pandas.computation.pytables import Expr
    from pandas.compat import string_types
    from datetime import datetime, timedelta
    import numpy as np

    """ allow backward compatibility for passed arguments """

    if isinstance(w, dict):
        w, op, value = w.get('field'), w.get('op'), w.get('value')
        if not isinstance(w, string_types):
            raise TypeError(
                "where must be passed as a string if op/value are passed")
        warnings.warn("passing a dict to Expr is deprecated, "
                      "pass the where as a single string",
                      DeprecationWarning)
    if isinstance(w, tuple):
        if len(w) == 2:
            w, value = w
            op = '=='
        elif len(w) == 3:
            w, op, value = w
        warnings.warn("passing a tuple into Expr is deprecated, "
                      "pass the where as a single string",
                      DeprecationWarning)

    if op is not None:
        if not isinstance(w, string_types):
            raise TypeError(
                "where must be passed as a string if op/value are passed")

        if isinstance(op, Expr):
            raise TypeError("invalid op passed, must be a string")
        w = "{0}{1}".format(w, op)
        if value is not None:
            if isinstance(value, Expr):
                raise TypeError("invalid value passed, must be a string")

            # stringify with quotes these values
            def convert(v):
                if isinstance(v, (basestring, datetime,np.datetime64,timedelta,np.timedelta64)) or hasattr(v, 'timetuple'):
                    return "'{0}'".format(str(v))
                return v

            if isinstance(value, (list,tuple)):
                value = [ convert(v) for v in value ]
            else:
                value = convert(value)

            w = "{0}{1}".format(w, value)

        warnings.warn("passing multiple values to Expr is deprecated, "
                      "pass the where as a single string",
                      DeprecationWarning)

    return w
