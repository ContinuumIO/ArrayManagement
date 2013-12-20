ArrayManagement
===============
THESE DOCS ARE HEAVILY OUT OF DATE....

Tools for working and ingesting different types of arrays.  This will be wrapped by a variety of servers 
[http](https://github.com/ContinuumIO/blaze-web), ZeroMQ, etc..

## Current design/architecture

The behavior can be summed up in a few main points

- This library provides an automatic way of constructing 
python objects to represent directories and files in the file system, 
csv files are turned into arrays, sql queries are turned into arrays, 
directories are turned into groups, containing other arrays.  A url path identifier 
is used for each object, based on the path to that object on the file system

- Simple customizations for how we map filesystem resources into python objects can be done inside a config.py
file, which can be dropped anywhere inside the hierarchy.  This directories and files inherit the configurations
of parent directories.  You can drop more config.py files to get more specific behavior deeper in the file system
hierarchy

- Simple customizations can be added by adding adding an `overrides` dictionary into the load.py file.  This will map
keys to parameters used to construct nodes (a tuple of class, kwargs for the class constructor).

- Simple customizations can also be added by adding globs into the `loaders` dictionary in the config.py file.  
This will map files matching the glob pattern, to parameters used to construct nodes - again, a tuple of the class, 
and kwargs for the class constructor)

- Advanced customizations can be added by dropping in a `load.py` file. The `load.py` file defines 2 functions, 
`keys` (think ls, or dir, or keys of a dictionary), and `get_node`. `get_node` returns some python object for the 
resource on the file system.  By modifying `keys`, and `get_node`, you stick arbitrary python objects into the url hierarchy.  One common use case of this is to return datasets that are derivatives of other datasets (think concatenating a bunch of csv files together).  

- There are a few utility objects right now - which probably need better names
    - PandasCacheableFixed - an object that can return a dataframe.  This is then cached in hdf5 using pandas
      fixed format - meaning, you cannot query it using pytables, but you can store things that pytables
      isn't very good at, like variable length strings
    - PandasCacheableTable - an object that can return a dataframe, which will then be stored in hdf5 using pandas
      table format.  This can be queried using PyTables expressions
    - PandasCSVNode - automatically loads csvs into pandas fixed format, using PandasCacheableFixed
    - PandasCSVTable - automatically loads csvs into pandas table format, using PandasCacheableTable
    - DirectorNode - You could customize this, but usually dropping in load.py files are sufficient
    - SimpleQueryTable - takes a sql query "myquery.sql", executes it, and caches the results in a pandas
      hdf5 table.


## Example Usage

We'll start with the examples directory - here is the filesystem hierarchy of the example directory (with my
hdf5 cache files, and other distractions like pyc files removed from the list)

    In [1]: !find example
    example
    example/custom
    example/custom/sample.csv
    example/custom/custom
    example/custom/load.py
    example/custom2
    example/custom2/sample.csv
    example/custom2/load.py
    example/csvs
    example/csvs/sample.csv
    example/csvs/sample2.CSV
    example/customcsvs
    example/customcsvs/sample.csv
    example/customcsvs/sample2.CSV
    example/customcsvs/config.py
    example/customcsvs/sample_pipe.csv
    example/pandashdf5
    example/pandashdf5/data.hdf5
    example/config.py

To begin, we construct a Client.
    
    In [14]: from arraymanagement.client import ArrayClient
    In [15]: c = ArrayClient('example')
    
    In [16]: c
    Out[16]: 
    type: ArrayClient
    urlpath: /
    filepath: .
    keys: ['custom', 'pandashdf5', 'csvs']
    
    In [17]: ls example
    config.py  config.pyc  csvs/  custom/  pandashdf5/


The contents of the example directory 
(3 directories, csvs, custom and pandashdf5 are mapped into 3 keys under the root directory.
The additional file, config.py, is a configuration file which is used by the various data ingest routines.
This configuration is inherited and can be overriden by nested directories by dropping in other config.py files.
We will begin by looking into the csvs directory.

    In [21]: ls example/csvs
    cache_sample2.hdf5  cache_sample.hdf5  sample2.CSV  sample.csv
    
    In [22]: c['csvs']
    Out[22]: 
    type: DirectoryNode
    urlpath: /csvs
    filepath: csvs
    keys: ['sample', 'sample2']
    
    In [23]: c['csvs']['sample']
    Out[23]: 
    type: PandasCSVNode
    urlpath: /csvs/sample
    filepath: csvs/sample.csv
    
    In [24]: c['csvs']['sample'].get()
    Out[24]: 
    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 73 entries, 0 to 72
    Data columns (total 2 columns):
    data      73  non-null values
    values    73  non-null values
    dtypes: int64(1), object(1)

#### Using configs.

The default configuration is setup to load all csv files (those matching `*.csv` or `*.CSV`) 
using the pandas fixed hdf5 format.  However, inside the example directory, we have added a config.py
overwriting this mapping for `*.CSV` files - as a result `*.csv` files are still stored in fixed format, however
`*.CSV` files are stored in table format.  As a result, we can query one CSV, but not the other.

    In [26]: c['csvs']['sample'].get().head()
    Out[26]: 
                      data  values
    0  2013-01-01 00:00:00       0
    1  2013-01-02 00:00:00       1
    2  2013-01-03 00:00:00       2
    3  2013-01-04 00:00:00       3
    4  2013-01-05 00:00:00       4
    
    In [27]: import pandas as pd
    
    In [28]: c['csvs']['sample2'].select(where=pd.Term('values','<',3))
    Out[28]: 
                      data  values
    0  2013-01-01 00:00:00       0
    1  2013-01-02 00:00:00       1
    2  2013-01-03 00:00:00       2

This is the config file that induces this behavior

    In [8]: !cat example/config.py
    from arraymanagement.nodes import csvnodes
    loaders = {
        '*.CSV' : csvnodes.PandasCSVTable,
        }

At it's core, the array management library maps python objects/nodes onto the file system.
We do this using glob filters.  The config file specifies glob filters of file patterns,
which define which class is used to construct the python object representing a resource on disk.
Currently, there are nodes for pandas hdf5 files, csvs, sql queries, and directories.
Next lets look inside the pandashdf5 directory

    In [29]: ls example/pandashdf5
    data.hdf5
    
    In [30]: c['pandashdf5']['data']
    Out[30]: 
    type: PandasHDFNode
    urlpath: /pandashdf5/data
    filepath: pandashdf5/data.hdf5
    keys: ['sample', 'testgroup']
    
    In [31]: c['pandashdf5']['data'].store
    Out[31]: 
    <class 'pandas.io.pytables.HDFStore'>
    File path: /home/hugoshi/work/ArrayManagement/example/pandashdf5/data.hdf5
    /sample                        frame_table  (typ->appendable,nrows->73,ncols->2,indexers->[index])
    /testgroup/dataset2            frame        (shape->[1,2])                                        

We have a common url system that can span multiple hdf5 files, and reference data inside them.  Inside the 
pandashdf5 directory, we have a data.hdf5 which has 2 datasets, one at /sample, and another at /testgroup/dataset2.

    In [32]: c['pandashdf5']
    Out[32]: 
    type: DirectoryNode
    urlpath: /pandashdf5
    filepath: pandashdf5
    keys: ['data']
    
    In [33]: c['pandashdf5/data']
    Out[33]: 
    type: PandasHDFNode
    urlpath: /pandashdf5/data
    filepath: pandashdf5/data.hdf5
    keys: ['sample', 'testgroup']
    
    In [34]: c['pandashdf5/data/sample']
    Out[34]: 
    type: PandasHDFNode
    urlpath: /pandashdf5/data
    filepath: pandashdf5/data.hdf5
    
    In [35]: c['pandashdf5/data/sample'].select()
    Out[35]: 
    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 73 entries, 0 to 72
    Data columns (total 2 columns):
    data      73  non-null values
    values    73  non-null values
    dtypes: int64(1), object(1)
    
    In [37]: c['pandashdf5/data/testgroup']
    Out[37]: 
    type: PandasHDFNode
    urlpath: /pandashdf5/data
    filepath: pandashdf5/data.hdf5
    keys: ['dataset2']
    
    In [38]: c['pandashdf5/data/testgroup/dataset2']
    Out[38]: 
    type: PandasHDFNode
    urlpath: /pandashdf5/data
    filepath: pandashdf5/data.hdf5
    
    In [39]: c['pandashdf5/data/testgroup/dataset2'].select()
    Out[39]: 
    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 73 entries, 0 to 72
    Data columns (total 2 columns):
    data      73  non-null values
    values    73  non-null values
    dtypes: int64(1), object(1)

As mentioned earlier, the array management library is a way to map python objects onto objects on disk.
You can write your own, by extending our classes or creating your own.  At the moment, there are a few 
relevant functions, and parameters that can be used to customize this behavior.

#### For Datasets

- `get_data`, which should return a dataframe, which we then cache in hdf5
- `load_data`, which is used for data which you can't load into memory.  
   sql queries for example, are usually executed under load_data, and 
   then written to hdf5 as we pull data off the cursor

#### For Data groups - Basic
`overrides` - This is a dictionary of keys to parameters.  If a key is in the dictionary, a node corresponding to the specified parameters
  is constructed
`loaders` - This is a dictionary of glob patterns to parameters.  If a file matches the glob pattern, a node is constructed according to the specified parameters
`pattern_priority` - This is a list of glob patterns - you can use this in case a file matches multiple glob patterns

#### For Data groups - Advanced
`keys` - should return a list of children
`get_node` - should turn urls into nodes.  This way you can define and construct your own custom node
directly

For example below, we create a create a custom key, which returns a specific CSV, with one of the columns transformed (multiplied by 6)
This `load.py` is dropped into the directory where you want that key to be available.

We do this to write all sorts of custom loaders - for example loaders which 
read directories full of csv files, and then parse the filename for a date, and 
add that as a column to the underlying data.  `load.py` files 
are loaded using imp.load_source - a feature or bug of this
is that that source code is read every time you try to access that directory - as a result, you can rapidly iterate on custom data ingest without having to restart the python interpreter, or call reload.

One other wierd thing we do, which turns out to be quite useful - you'll notice that `get_factor_data` is written as an instance method.  It actually gets patched in
as the get_data method of that particular instance representing that dataset.

    In [14]: ls example/custom2
    load.py  sample.csv

    In [15]: cat example/custom2/load.py 
    
    import pandas as pd
    from arraymanagement.nodes.hdfnodes import PandasCacheableTable

    def get_factor_data(self):
        path = self.joinpath('sample.csv')
        data = pd.read_csv(path)
        data['values'] = data['values'] * 6
        return data

    overrides = {'sample2' : (PandasCacheableTable, {'get_data' : get_factor_data})}
    

    In [18]: c['custom2']['sample'].get().head()
    Out[18]: 
                      data  values
    0  2013-01-01 00:00:00       0
    1  2013-01-02 00:00:00       1
    2  2013-01-03 00:00:00       2
    3  2013-01-04 00:00:00       3
    4  2013-01-05 00:00:00       4

    In [20]: c['custom2']['sample2'].select().head()
    Out[20]: 
                      data  values
    0  2013-01-01 00:00:00       0
    1  2013-01-02 00:00:00       6
    2  2013-01-03 00:00:00      12
    3  2013-01-04 00:00:00      18
    4  2013-01-05 00:00:00      24


## Long Term Features/Vision

### Data Ingest

- Ingest of different types of txt files (csv, json)
- Read a variety of file based storage mechanisms (hdf5, pandas hdf5 store, blz)
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

