![Amphora Logo](/doc/_static/amphora.png)

xarray_mongodb
==============
[![doc-badge](https://github.com/AmphoraInc/xarray_mongodb/workflows/Documentation/badge.svg)](https://github.com/AmphoraInc/xarray_mongodb/actions)
[![lint-badge](https://github.com/AmphoraInc/xarray_mongodb/workflows/Lint/badge.svg)](https://github.com/AmphoraInc/xarray_mongodb/actions)
[![pytest-badge](https://github.com/AmphoraInc/xarray_mongodb/workflows/Test%20latest/badge.svg)](https://github.com/AmphoraInc/xarray_mongodb/actions)

Read the full documentation at https://xarray-mongodb.readthedocs.io/en/latest/


xarray_mongodb allows storing xarray objects on MongoDB. Its design is heavily
influenced by GridFS.

Current Features
----------------
- Synchronous operations with PyMongo
- asyncio support with Motor
- Units annotation with Pint
- Delayed put/get of xarray objects backed by dask.
  Only metadata and numpy-backed variables (e.g. indices) are written and read
  back at the time of graph definition.
- Support for dask distributed
- Data is stored on the database in a format that is agnostic to Python;
  this allows writing clients in different languages.

Upcoming Features
-----------------
- sparse arrays
