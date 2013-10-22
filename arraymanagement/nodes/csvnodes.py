from .hdfnodes import PandasCacheableFixed, PandasCacheableTable
import pandas as pd
from os.path import basename, splitext, join, dirname

class PandasCSVNode(PandasCacheableFixed):
    is_group = False
    def __init__(self, context, csv_options=None):
        options = kwargs.pop(csv_options, None)
        super(PandasCacheable, self).__init__(context)
        if options is None:
            self.options = self.config.get('csv_options')
        else:
            self.options = options

    def _get_data(self):
        fname = join(self.basepath, self.relpath)
        data = pd.read_csv(fname, **self.options)
        return data

class PandasCSVTable(PandasCacheableTable):
    is_group = False
    def __init__(self, context, csv_options=None):
        options = kwargs.pop(csv_options, None)
        super(PandasCacheable, self).__init__(context)
        if options is None:
            self.options = self.config.get('csv_options')
        else:
            self.options = options

    def _get_data(self):
        fname = join(self.basepath, self.relpath)
        data = pd.read_csv(fname, **self.options)
        return data
