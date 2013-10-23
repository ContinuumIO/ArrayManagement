import nodes.hdfnodes as hdfnodes
def clear_mem_cache():
    hdfnodes.pandas_hdf5_cache.clear()
    hdfnodes.PandasCacheableFixed.inmemory_cache.clear()

