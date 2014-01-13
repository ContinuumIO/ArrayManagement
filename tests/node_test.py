import tempfile
import datetime as dt
from os.path import join, dirname, split, realpath, exists
from os import makedirs
import json

from arraymanagement.client import ArrayClient

def setup_module():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    client.clear_disk_cache()

def teardown_module():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    client.clear_disk_cache()

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

def test_sql_node():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    aapl = client['/sqlviews/AAPL'].select()
    assert aapl.shape == (3925,3)
    
def test_sql_cache():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    #query 2012 data
    aapl = client['/sqlviews/view'].select(ticker='AAPL',
                                           date=[dt.datetime(2012,1,1), 
                                                 dt.datetime(2012,12,30)],
                                           where=[]
                                           )
    assert aapl.date.max() == dt.datetime(2012, 12, 28)
    assert aapl.shape == (249,  3)
    assert client['/sqlviews/view'].table.nrows == 249

    #query 2012 data, make sure we didn't add more data to hdf5
    aapl = client['/sqlviews/view'].select(ticker='AAPL',
                                           date=[dt.datetime(2012,1,1), 
                                                 dt.datetime(2012,12,30)],
                                           where=[]
                                           )
    assert aapl.date.max() == dt.datetime(2012, 12, 28)    
    assert client['/sqlviews/view'].table.nrows == 249

    #query 2013 data, make sure we DID add more data to hdf5
    aapl = client['/sqlviews/view'].select(ticker='AAPL',
                                           date=[dt.datetime(2013,1,1), 
                                                 dt.datetime(2013,12,30)],
                                           where=[]
                                           )
    assert aapl.date.max() == dt.datetime(2013, 8, 9)
    #query 2012 data, make sure it still works
    aapl = client['/sqlviews/view'].select(ticker='AAPL',
                                           date=[dt.datetime(2012,1,1), 
                                                 dt.datetime(2012,12,30)],
                                           where=[]
                                           )
    assert aapl.date.max() == dt.datetime(2012, 12, 28)
    
    #try a query with a where clause
    aapl = client['/sqlviews/view'].select(ticker='AAPL',
                                           date=[dt.datetime(2013,1,1), 
                                                 dt.datetime(2013,12,30)],
                                           where=[('c', '==', 513.985)]
                                           )
    assert aapl.shape == (1,3) and aapl.c.iloc[0] == 513.985
