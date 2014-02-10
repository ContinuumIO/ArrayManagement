import tempfile
import datetime as dt
from os.path import join, dirname, split, realpath, exists
from os import makedirs
import json
from sqlalchemy.sql import and_


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

def test_sql_new_cache():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    #query 2012 data
    aapl = client['/sqlviews/bulkview.bsqlspec'].select(ticker='AAPL',
                                       date=[dt.datetime(2003,1,1),
                                             dt.datetime(2012,12,30)],
                                       )
    arr = client['/sqlviews/flex_view.fsql']
    aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(2003,1,1), \
                                arr.date <= dt.datetime(2012,12,30)))
    cache = join(basepath,'sqlviews','.cache','cache_flex_view.fsql.hdf5')

    #minimum shifted left
    aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(1998,1,1), \
                                arr.date <= dt.datetime(2012,12,30)))

    #minimum and maximum shifted left
    aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(1998,1,1), \
                                arr.date <= dt.datetime(2001,12,30)))

    #maximum shifted right
    aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(2003,1,1), \
                                arr.date <= dt.datetime(2013,12,30)))

    #maximum and minimum shifted right
    aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(2013,1,1), \
                                arr.date <= dt.datetime(2013,12,30)))

    #inner call
    aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(2004,1,1), \
                                arr.date <= dt.datetime(2009,12,30)))

    print aapl.head()


def test_pytables_access():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)
    assert client["/pytables/array.hdf5/firstgroup"].keys() == []
    assert client["/pytables/array.hdf5/random_numbers"].node[:].shape == (300,200,100)

