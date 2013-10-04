import tempfile
from os.path import join, dirname, split, realpath, exists
from os import makedirs
import json

from arraymanagement.client import ArrayClient
def test_csv_node():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    node = client.get_node('/csvs/sample')
    data = node.get()

