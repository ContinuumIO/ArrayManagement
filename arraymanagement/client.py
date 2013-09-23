import databag
import os
from os.path import join, dirname

class ArrayClient(object):
    def __init__(self, path):
        self.root = path
        self.md = databag.DataBag(join(sel.root, "__md.db"))
    
        

