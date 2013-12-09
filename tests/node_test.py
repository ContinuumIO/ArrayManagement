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
    node = client.get_node('/custom/sample2')
    data1 = node.select()
    node = client.get_node('/custom/sample')
    data2 = node.get()
    assert data2.iloc[2]['values'] == 2
    assert data1.iloc[2]['values'] == 4


def test_csv_node():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    node = client.get_node('/customcsvs/sample')
    data1 = node.get()
    node = client.get_node('/customcsvs/sample2')
    data2 = node.select()
    node = client.get_node('/customcsvs/sample_pipe')
    data3 = node.select()
    #better check later
