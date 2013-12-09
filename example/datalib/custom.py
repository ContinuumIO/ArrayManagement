from arraymanagement.nodes.hdfnodes import PandasCacheableTable
import collections

class MyCSVNode(PandasCacheableTable):
    is_group = False
    def get_data(self):
        fname = join(self.basepath, self.relpath)
        data = pd.read_csv(fname, **self.config.get('csv_options'))
        data['values'] = data['values'] * 2
        return data


loaders = collections.OrderedDict([
        ('sample2', MyCSVNode)
        ])

config = {'loaders' : loaders}
