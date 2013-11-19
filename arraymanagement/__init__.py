import nodes.hdfnodes as hdfnodes
import tables.file

def clear_mem_cache():
    hdfnodes.pandas_hdf5_cache.clear()
    hdfnodes.PandasCacheableFixed.inmemory_cache.clear()
    tables.file.close_open_files()
