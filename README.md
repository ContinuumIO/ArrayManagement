ArrayManagement
===============

Tools for working and ingesting different types of arrays.  This will be wrapped by a variety of servers 
[http](https://github.com/ContinuumIO/blaze-web), ZeroMQ, etc..


## Features

### Data Ingest

- Ingest of different types of txt files (csv, json)
- Read a variety of file based storage mechanisms (hdf5, pandas hdf5 store, blz)
- Custom loaders that can be dropped in via python 
  (for example: load all csvs in the directory, parse the filename to determine the date, add that as a column)
- parameterized SQL queries, along with a caching mechanism build on top.

### Data Access 
- view datasets in the web, easily load them into Excel and python
- Ability to slice data, using standard numpy expressions, and retrieve only subsets of the data.

### Data Management
- versioning of datasets, datasets can be altered, and then tagged as different versions
- searchable metadata that can be added to datasets.
- typical unix user/group system, which gives permissions to different directories/datasets.  
  Users can create api keys that their applications use, and manage/delete/change api keys

### Advanced Blaze Features
- full blaze expression graph evaluation over remote data

## Some Technical Details
- Metadata search system, built on top of [databag](https://github.com/nod/databag).  Databag is a json
  store with mongo style queries, built on top of sqlite.  I was considering [ejdb](http://ejdb.org/), however
  I'd rather not add non-python dependencies if we don't need them.
- Data Ingest
  - Some data formats require an intermediate representation (csv/json should be parsed and saved to hdf5)
    - These will be written to a __init__.hdf5 file 

  - Other formats require no intermediate representation.  We will try to do the right thing heuristically, by 
    looking at extensions, but people can customize settings using the config
  - load.py can be dropped into the directory - load.py, if dropped in will determine how to parse/load data for this path
  - load.py for now, writes results into an __init__.hdf5 file, which we know how to understand
  - config.py can specify a configuration for the directory
  - config.py should propagate to directory children, load.py does not, unless the config asks it to.
  - functions.py provide data access functions.  We will provide some utilities to easily build functions which will:
    - parameterize sql queries
    - cache the results in __init__.hdf5 files
    - compute the parameterized results from the hdf5 cache
- Data Slicing
  - We support basic numpy style slicing, column selections for all array types.  
  - We support more sophisticated stuff for blaze/dynd
- Data computation
  - for pandas data, we just ship the entire dataframe (after any slicing has been done)
  - For blaze, we do expression graph stuff

## Example Usage


    In [1]: pwd
    Out[1]: u'/home/hugoshi/work/ArrayManagement'
    
    In [2] from arraymanagement.client import ArrayClient
    
    In [3]: c = ArrayClient('example')
    
    In [4]: c.keys()
    Out[4]: ['custom', 'pandashdf5', 'csvs']
    
    In [5]: ls example
    config.py  config.pyc  csvs/  custom/  pandashdf5/
    


The contents of the example directory (3 directories, csvs, custom and pandashdf5 are mapped into 3 keys under the root directory.  The additional 
file, config.py, is a configuration file which is used by the various data ingest routines.  This configuration is inherited and 
can be overriden by nested directories by dropping in other config.py files.


    In [10]: ls example/csvs
    cache_sample2.hdf5  cache_sample.hdf5  sample2.CSV  sample.csv
    
    In [12]: c['csvs']
    Out[12]: DirectoryNode:/csvs:csvs
    
    In [13]: c['csvs'].keys()
    Out[13]: ['sample', 'cache_sample2', 'cache_sample', 'sample2']
    
    In [14]: c['csvs']['sample']
    Out[14]: PandasCSVNode:/csvs/sample:csvs/sample.csv
    
    In [15]: c['csvs']['sample'].get()
    Out[15]: 
    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 73 entries, 0 to 72
    Data columns (total 2 columns):
    data      73  non-null values
    values    73  non-null values
    dtypes: int64(1), object(1)


In the csvs directory we have 2 cached hdf5 files (we have classes designed to read CSVs and cache them to hdf5)
Pandas has 2 types of hdf5 storage, a fixed format which is not queryable, you can only load the whole thing
from disk at once, and one based on a PyTables table, which can be queried in PyTables fashion.  Supporting both
are important, as the inability to handle vlen strings efficienlty in pytables tables can massively expand datasets in 
memory.  The first less fexible storage mechanism is the Fixed format, versus the Table format


    In [16]: c['csvs']['sample'].get().head()
    Out[16]: 
                      data  values
    0  2013-01-01 00:00:00       0
    1  2013-01-02 00:00:00       1
    2  2013-01-03 00:00:00       2
    3  2013-01-04 00:00:00       3
    4  2013-01-05 00:00:00       4
    
    In [17]: import pandas as pd
    
    In [19]: c['csvs']['sample2'].select(where=pd.Term('values','<',3))
    Out[19]: 
                      data  values
    0  2013-01-01 00:00:00       0
    1  2013-01-02 00:00:00       1
    2  2013-01-03 00:00:00       2



We have 2 csv files in this directory, sample.csv and sample2.CSV.  The default configuration is setup to load all CSVs using the fixed format.
however in our config.py, we overwrote to the pattern to load *.CSV in the TableFormat



    In [8]: !cat example/config.py
    from arraymanagement.nodes import csvnodes
    loaders = {
        '*.CSV' : csvnodes.PandasCSVTable,
        }


At it's core, the array management library maps nodes onto the file system.  We do this using glob filters.  The config file
can specify glob filters of file patterns, which define which class is used to construct the node representing a resource on disk.
Currently, there are nodes for pandas hdf5 files, csvs, sql queries, and directories.



    In [21]: ls example/pandashdf5
    data.hdf5
    
    In [22]: c['pandashdf5']['data']
    Out[22]: PandasHDFNode:/pandashdf5/data:pandashdf5/data.hdf5
    
    In [25]:  c['pandashdf5']['data'].store
    Out[25]: 
    <class 'pandas.io.pytables.HDFStore'>
    File path: /home/hugoshi/work/ArrayManagement/example/pandashdf5/data.hdf5
    /sample                    frame_table  (typ->appendable,nrows->73,ncols->2,indexers->[index])
    /testdir/sample            frame        (shape->[1,2])                                        


We extend the directory - url mapping inside the hdf5 file, so pandashdf5/data/sample and 
pandashdf5/data/testdir/sample are both valid urls.  On the file system, pandashdf5 is a directory, 
data.hdf5 is an hdf5 file, and testdir is an hdf5 group within the dataset.  This way we have a common
url system that can span multiple hdf5 files


    In [26]:  c['pandashdf5']['data'].keys()
    Out[26]: ['sample', 'testdir']
    
    In [27]:  c['pandashdf5']['data']['sample']
    Out[27]: PandasHDFNode:/pandashdf5/data:pandashdf5/data.hdf5
    
    In [28]:  c['pandashdf5']['data']['testdir']
    Out[28]: PandasHDFNode:/pandashdf5/data:pandashdf5/data.hdf5
    
    In [29]:  c['pandashdf5']['data']['testdir'].keys()
    Out[29]: ['sample']
    
    In [31]:  c['pandashdf5']['data']['testdir']['sample']
    Out[31]: PandasHDFNode:/pandashdf5/data:pandashdf5/data.hdf5
    
    In [32]:  c['pandashdf5']['data']['testdir']['sample'].select()
    Out[32]: 
    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 73 entries, 0 to 72
    Data columns (total 2 columns):
    data      73  non-null values
    values    73  non-null values
    dtypes: int64(1), object(1)



As mentioned earlier, the array management library is a way to map python objects onto objects on disk.
You can write your own, by extending our classes or creating your own.  At the moment, there are a few 
relvant functions.

For Datasets:

_get_data, which should return a dataframe, which we then cache in hdf5
load_data, which is used for data which you can't load into memory.  sql queries for example, are usually executed
under load_data, and then written to hdf5 as we pull data off the cursor

For Data groups:

keys - should return a list of children
get_node - should turn urls into nodes.  This way you can define and construct your own custom node
directly

For example below, we create a custom node which reads one of the previous csv files, but multiplies a column by 2.

This load.py is dropped into the directory where you want that key to be available.

We do this to write all sorts of custom loaders - for example loaders which read directories full of csv files, and then parse the filename for a date, and 
add that as a column to the underlying data.


    In [33]: ls custom
    ls: cannot access custom: No such file or directory
    
    In [34]: ls example/custom
    cache_sample2.hdf5  cache_sample.hdf5  load.py  load.pyc  sample.csv
    
    In [35]: cat example/custom/load.py
    import posixpath
    from os.path import join, relpath
    import pandas as pd
    
    from arraymanagement import default_loader
    from arraymanagement.nodes.hdfnodes import PandasCacheableTable
    
    
    keys = default_loader.keys
    
    old_get_node = default_loader.get_node
    
    class MyCSVNode(PandasCacheableTable):
        is_group = False
        def _get_data(self):
            fname = join(self.basepath, self.relpath)
            data = pd.read_csv(fname, **self.config.get('csv_options'))
            data['values'] = data['values'] * 2
            return data
    
    def get_node(urlpath, rpath, basepath, config):
        key = posixpath.basename(urlpath)
        if key == 'sample2':
            fname = "sample.csv"
            new_rpath = relpath(join(basepath, rpath, 'sample.csv'), basepath)
            return MyCSVNode(urlpath, new_rpath, basepath, config)        
        else:
            return old_get_node(urlpath, rpath, basepath, config)
    
    In [37]: c['custom']['sample2']
    Out[37]: MyCSVNode:/custom/sample2:custom/sample.csv
    
    In [38]: c['custom']['sample2'].select()
    Out[38]: 
    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 73 entries, 0 to 72
    Data columns (total 2 columns):
    data      73  non-null values
    values    73  non-null values
    dtypes: int64(1), object(1)
    
    In [39]: c['custom']['sample2'].select().head(0
       ....: )
    Out[39]: 
    Empty DataFrame
    Columns: [data, values]
    Index: []
    
    In [40]: c['custom']['sample2'].select().head()
    Out[40]: 
                      data  values
    0  2013-01-01 00:00:00       0
    1  2013-01-02 00:00:00       2
    2  2013-01-03 00:00:00       4
    3  2013-01-04 00:00:00       6
    4  2013-01-05 00:00:00       8
    
    In [41]: 

