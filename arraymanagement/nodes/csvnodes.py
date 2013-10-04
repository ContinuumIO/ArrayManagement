from . import PandasCacheable
from .hdfnodes import HDFDataSetMixin
import pandas as pd
from os.path import basename, splitext, join, dirname

class PandasCSVNode(PandasCacheable):
    is_group = False
    def _get_data(self):
        fname = join(self.basepath, self.relpath)
        data = pd.read_csv(fname, **self.config.get('csv_options'))
        return data
