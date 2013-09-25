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
