import tempfile
from os.path import join, dirname, split, realpath, exists
from os import makedirs
import json

from arraymanagement.serialization import jsdump
from arraymanagement.pathutils import recursive_config_load

def test_jsload():
    path = tempfile.mkdtemp() # we will read this path
    path1 = join(path, 'foo') # and this one
    path2 = join(path, 'bar') # not this one
    path3 = join(path1, 'empty') # not this one
    path4 = join(path3, 'baz')# yes to this one
    makedirs(path1)
    makedirs(path2)
    makedirs(path3)
    makedirs(path4)
    # write data in /foo, /foo/bar, and /baz
    # ensure that we read updates from path, path/foo, path/foo/bar
    # and not from path/baz
    jsdump({'a':1,'b':2}, join(path, "config.json"))
    jsdump({'b':3}, join(path1, "config.json"))
    jsdump({'c':4}, join(path2, "config.json"))
    jsdump({'b':5, 'd':1}, join(path4, "config.json"))
    data = recursive_config_load(path4, path)
    assert data['a'] == 1
    assert data['b'] == 5
    assert data['d'] == 1
    assert data.get('c') is None
    
        
    
    
