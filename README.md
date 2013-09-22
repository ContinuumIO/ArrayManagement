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
