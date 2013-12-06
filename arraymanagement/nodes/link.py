from . import Node
class Link(Node):
    def __init__(self, context, linkurl=None):
        super(Link, self).__init__(context)
        self.linkurl = linkurl
        self.link_node = self[linkurl]

    def repr_data(self):
        info = super(Link, self).repr_data()
        info.append ("link_url: %s" % self.link_node.urlpath)
        return info

    def __getattr__(self, name):
        return getattr(self.link_node, name)

