from os.path import join, dirname, split, realpath, exists, abspath
import imp
import posixpath
from .serializations import jsload

def urlsplit(path, basepath, maxdepth=10):
    """splits /home/hugo/foo/bar/baz into foo, bar, baz, assuming
    /home/hugo is the basepath
    """
    if maxdepth == 0:
        return []
    if path == basepath:
        return []
    else:
        urlpath, path = posixpath.split(path)
        results = urlsplit(urlpath, basepath, maxdepth=maxdepth - 1)
        results.append(path)
        return results

def _dirsplit(path, basepath, maxdepth=10):
    if maxdepth == 0:
        return []
    if path == basepath:
        return []
    else:
        dirpath, path = split(path)
        if path == "":
            return []
        else:
            results = dirsplit(dirpath, basepath, maxdepth=maxdepth - 1)
            results.append(path)
            return results

def dirsplit(path, basepath, maxdepth=10):
    """splits /home/hugo/foo/bar/baz into foo, bar, baz, assuming
    /home/hugo is the basepath
    """
    path = realpath(path)
    basepath = realpath(basepath)
    return _dirsplit(path, basepath, maxdepth=maxdepth)

# def get_config(path):
#     fpath = join(path, "config.json")
#     if exists(fpath):
#         return jsload(fpath)
#     else:
#         return {}
        
def get_config(path, basepath):
    fpath = abspath(path)
    if exists(fpath):
        directories = dirsplit(path, basepath)
        name = ".".join(directories)
        mod = imp.load_source(name, fpath)
        return mod.__dict__
    else:
        return {}

def recursive_config_load(path, basepath):
    """recursively loads configs as we traverse from basepath to path
    """
    incremental_paths = dirsplit(path, basepath)
    config = get_config(basepath, basepath)
    for idx in range(len(incremental_paths)):
        currpath = join(basepath, *incremental_paths[:idx + 1])
        conf = get_config(currpath, basepath)
        config.update(conf)
    return config
