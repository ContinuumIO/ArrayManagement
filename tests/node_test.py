import tempfile
import datetime as dt
from os.path import join, dirname, split, realpath, exists
from os import makedirs
import json
from sqlalchemy.sql import and_, column
import pandas as pd


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

def test_sql_yaml_cache():
    basepath = join(dirname(dirname(__file__)), 'example')
    client = ArrayClient(basepath)

    arr = client['/sqlviews/example_sql.yaml']

    date_1 = dt.datetime(2000,1,1)
    date_2 = dt.datetime(2003,12,30)
    aapl = arr.select(and_(arr.ticker=='AAPL'),date_1 = date_1 , date_2 = date_2)

    query = arr.select(and_(arr.ticker=='AAPL'),date_1 = date_1 , date_2 = date_2, IgnoreCache=True)

    query = arr.select(and_(arr.ticker=='AAPL'), IgnoreCache=True)

    arr = client['/sqlviews/example_no_dates.yaml']
    aapl = arr.select(and_(arr.ticker.in_(['A','AA'])))
    print aapl

    arr = client['/sqlviews/example_no_dates_not_entities.yaml']
    aapl = arr.select(query_filter=None)
    print aapl

    query = arr.select(query_filter=None, IgnoreCache=True)




# def test_sql_node():
#     basepath = join(dirname(dirname(__file__)), 'example')
#     client = ArrayClient(basepath)
#     aapl = client['/sqlviews/AAPL'].select()
#     assert aapl.shape == (3925,3)
#
# def test_sql_cache():
#     basepath = join(dirname(dirname(__file__)), 'example')
#     client = ArrayClient(basepath)
#     #query 2012 data
#     aapl = client['/sqlviews/view'].select(ticker='AAPL',
#                                            date=[dt.datetime(2012,1,1),
#                                                  dt.datetime(2012,12,30)],
#                                            where=[]
#                                            )
#     assert aapl.date.max() == dt.datetime(2012, 12, 28)
#     assert aapl.shape == (249,  3)
#     assert client['/sqlviews/view'].table.nrows == 249
#
#     #query 2012 data, make sure we didn't add more data to hdf5
#     aapl = client['/sqlviews/view'].select(ticker='AAPL',
#                                            date=[dt.datetime(2012,1,1),
#                                                  dt.datetime(2012,12,30)],
#                                            where=[]
#                                            )
#     assert aapl.date.max() == dt.datetime(2012, 12, 28)
#     assert client['/sqlviews/view'].table.nrows == 249
#
#     #query 2013 data, make sure we DID add more data to hdf5
#     aapl = client['/sqlviews/view'].select(ticker='AAPL',
#                                            date=[dt.datetime(2013,1,1),
#                                                  dt.datetime(2013,12,30)],
#                                            where=[]
#                                            )
#     assert aapl.date.max() == dt.datetime(2013, 8, 9)
#     #query 2012 data, make sure it still works
#     aapl = client['/sqlviews/view'].select(ticker='AAPL',
#                                            date=[dt.datetime(2012,1,1),
#                                                  dt.datetime(2012,12,30)],
#                                            where=[]
#                                            )
#     assert aapl.date.max() == dt.datetime(2012, 12, 28)
#
#     #try a query with a where clause
#     aapl = client['/sqlviews/view'].select(ticker='AAPL',
#                                            date=[dt.datetime(2013,1,1),
#                                                  dt.datetime(2013,12,30)],
#                                            where=[('c', '==', 513.985)]
#                                            )
#     assert aapl.shape == (1,3) and aapl.c.iloc[0] == 513.985
#
# def test_sql_new_cache():
#     basepath = join(dirname(dirname(__file__)), 'example')
#     client = ArrayClient(basepath)
#
#     #query 2012 data
#     aapl = client['/sqlviews/bulkview.bsqlspec'].select(ticker='AAPL',
#
#                                        date=[dt.datetime(2003,1,1),
#                                              dt.datetime(2012,12,30)],
#                                        )
#
#
#     arr = client['/sqlviews/flex_view.fsql']
#
#     aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(1998,1,1), \
#                                 arr.date <= dt.datetime(1998,12,30)))
#
#
#     arr = client['/sqlviews/flex_view.fdsql']
#     store = arr.store
#
#     print '\n\n\nFull Selection'
#
#     date_1 = dt.datetime(2000,1,1)
#     date_2 = dt.datetime(2003,12,30)
#     aapl = arr.select(and_(arr.ticker=='AAPL'),date_1 = date_1 , date_2 = date_2)
#
#     max_date = store['/cache_spec']['end_date'].max()
#
#     assert aapl.date.max() == date_2
#     assert max_date == pd.Timestamp(date_2)
#
#
#
#     print '\n\n\nFull Right'
#     aapl = arr.select(and_(arr.ticker=='AAPL'), date_1 = dt.datetime(2004,1,1), \
#                                 date_2 = dt.datetime(2005,12,30))
#
#     print '\n\n\nShifted Right'
#     aapl = arr.select(and_(arr.ticker=='AAPL'), date_1 = dt.datetime(2004,12,30), \
#                                 date_2 = dt.datetime(2008,12,30))
#     print '\n\n\nInner Selection'
#     aapl = arr.select(and_(arr.ticker=='AAPL'), date_1 = dt.datetime(2006,12,30), \
#                                 date_2 = dt.datetime(2007,12,30))
#
#     print '\n\n\nShifted left'
#     aapl = arr.select(and_(arr.ticker=='AAPL'), date_1 = dt.datetime(1999,10,30), \
#                                 date_2 = dt.datetime(2006,4,30))
#
#     print '\n\n\nFull left'
#     aapl = arr.select(and_(arr.ticker=='AAPL'), date_1 = dt.datetime(1999,1,1), \
#                                 date_2 = dt.datetime(1999,6,30))
#     print '\n\n\nFull Outer'
#     aapl = arr.select(and_(arr.ticker=='AAPL'), date_1 = dt.datetime(1998,1,05), \
#                                 date_2 = dt.datetime(2013,8,9))
#
#     # cache = join(basepath,'sqlviews','.cache','cache_flex_view.fdsql.hdf5')
#
#     # #minimum shifted left
#     # aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(1998,1,1), \
#     #                             arr.date <= dt.datetime(2012,12,30)))
#     #
#     # #minimum and maximum shifted left
#     # aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(1998,1,1), \
#     #                             arr.date <= dt.datetime(2001,12,30)))
#     #
#     # #maximum shifted right
#     # aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(2003,1,1), \
#     #                             arr.date <= dt.datetime(2013,12,30)))
#     #
#     # #maximum and minimum shifted right
#     # aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(2013,1,1), \
#     #                             arr.date <= dt.datetime(2013,12,30)))
#     #
#     # #inner call
#     # aapl = arr.select(and_(arr.ticker=='AAPL',arr.date >= dt.datetime(2004,1,1), \
#     #                             arr.date <= dt.datetime(2009,12,30)))
#
#     print aapl.head()
