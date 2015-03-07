from .nodes import hdfnodes
from tables import file

def clear_mem_cache():
    for key in hdfnodes.pandas_hdf5_cache.keys():
        store = hdfnodes.pandas_hdf5_cache.pop(key)
        store.close()
    hdfnodes.PandasCacheableFixed.inmemory_cache.clear()
