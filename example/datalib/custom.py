from arraymanagement.nodes.hdfnodes import PandasCacheableTable
import pandas as pd
from posixpath import join
import collections

def get_data(self):
    data = self["../sample.csv"].get().copy()
    data['values'] = data['values'] * 2
    return data


loaders = collections.OrderedDict([
        ('sample2', (PandasCacheableTable, {'get_data' : get_data}))
        ])

config = {'loaders' : loaders}
