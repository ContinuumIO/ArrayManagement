import posixpath
from os.path import join, relpath
import pandas as pd

from arraymanagement import default_loader
from arraymanagement.nodes.hdfnodes import PandasCacheableTable
from arraymanagement.nodes import NodeContext

def get_factor_data(self):
    path = self.joinpath('sample.csv')
    data = pd.read_csv(path)
    data['values'] = data['values'] * 6
    return data

overrides = {'sample2' : (PandasCacheableTable, {'get_data' : get_factor_data})}
