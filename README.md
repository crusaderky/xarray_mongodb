xarray_mongodb
==============
[![doc-badge](https://github.com/crusaderky/xarray_mongodb/actions/workflows/docs.yml/badge.svg)](https://github.com/crusaderky/xarray_mongodb/actions)
[![pre-commit-badge](https://github.com/crusaderky/xarray_mongodb/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/crusaderky/xarray_mongodb/actions)
[![pytest-badge](https://github.com/crusaderky/xarray_mongodb/actions/workflows/pytest.yml/badge.svg)](https://github.com/crusaderky/xarray_mongodb/actions)
[![codecov-badge](https://codecov.io/gh/crusaderky/xarray_mongodb/branch/main/graph/badge.svg)](https://codecov.io/gh/crusaderky/xarray_mongodb/branch/main)

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
