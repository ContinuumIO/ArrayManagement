from .hdfnodes import PandasCacheableFixed, PandasCacheableTable, PandasCacheable
import pandas as pd
from os.path import basename, splitext, join, dirname

class PandasCSVNode(PandasCacheableFixed):
    is_group = False
    def __init__(self, context, csv_options=None):
        super(PandasCSVNode, self).__init__(context)
        if csv_options is None:
            self.csv_options = self.config.get('csv_options')
        else:
            self.csv_options = csv_options

    def get_data(self):
        fname = join(self.basepath, self.relpath)
        data = pd.read_csv(fname, **self.csv_options)
        return data

class PandasCSVTable(PandasCacheableTable):
    is_group = False
    def __init__(self, context, csv_options=None):
        super(PandasCSVTable, self).__init__(context)
        if csv_options is None:
            self.csv_options = self.config.get('csv_options')
        else:
            self.csv_options = csv_options

    def get_data(self):
        fname = join(self.basepath, self.relpath)
        data = pd.read_csv(fname, **self.csv_options)
        return data
