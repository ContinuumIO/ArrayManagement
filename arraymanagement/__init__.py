import nodes.hdfnodes as hdfnodes
import tables.file

def clear_mem_cache():
    hdfnodes.pandas_hdf5_cache.clear()
    hdfnodes.PandasCacheableFixed.inmemory_cache.clear()
    close_pytables()

def close_pytables():
    _open_files = tables.file._open_files
    are_open_files = len(_open_files) > 0
    for fname, fileh in _open_files.items():
        fileh.close()


