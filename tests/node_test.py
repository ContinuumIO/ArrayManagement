import tempfile
from os.path import join, dirname, split, realpath, exists
from os import makedirs
import json

from arraymanagement.client import ArrayClient
def test_csv_node():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    node = client.get_node('/csvs/sample')
    data = node.select()
    #better check later
    assert data.shape == (73,2)

def test_hdf_node():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    node = client.get_node('/pandashdf5/data')
    assert 'sample' in node.keys()
    node = node.get_node('sample')
    data = node.select()
    assert data.shape == (73,2)

def test_custom_node():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    node = client.get_node('/custom/custom/sample2')
    data1 = node.select()
    node = client.get_node('/custom/custom/sample')
    data2 = node.select()
    assert data2.iloc[2]['values'] == 2
    assert data1.iloc[2]['values'] == 4
