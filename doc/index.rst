xarray_mongodb
==============

**xarray_mongodb** allows storing `xarray <http://xarray.pydata.org>`_
objects on
MongoDB. Its design is heavily influenced by
`GridFS <https://docs.mongodb.com/manual/core/gridfs/>`_.


Current Features
----------------
- Synchronous operations with
  `PyMongo <https://api.mongodb.com/python/current/>`_
- asyncio support with `Motor <https://motor.readthedocs.io/>`_
- Units annotations with `Pint <https://pint.readthedocs.io/>`_
- Delayed put/get of xarray objects backed by `dask <https://dask.org/>`_.
  Only metadata and numpy-backed variables (e.g. indices) are written and read
  back at the time of graph definition.
- Support for `dask distributed <https://distributed.dask.org/>`_.
  Note that the full init parameters of the MongoDB client are sent over the
  network; this includes access credentials. One needs to make sure that
  network communications between dask client and scheduler and between
  scheduler and workers are secure.
- Data is stored on the database in a format that is agnostic to Python;
  this allows writing clients in different languages.


Upcoming Features
-----------------
- Sparse arrays with `Sparse <https://sparse.pydata.org/>`_


Limitations
-----------
- The Motor Tornado driver is not supported due to lack of developer
  interest - submissions are welcome.
- At the moment of writing, Dask and Pint are not supported at the same time due to
  limitations in the Pint and xarray packages.
- ``attrs`` are limited to the data types natively accepted by PyMongo
- Non-string xarray dimensions and variable names are not supported


Quick start
-----------
.. code-block:: python

   >>> import pymongo
   >>> import xarray
   >>> import xarray_mongodb

   >>> db = pymongo.MongoClient()['mydb']
   >>> xdb = xarray_mongodb.XarrayMongoDB(db)
   >>> a = xarray.DataArray([1, 2], dims=['x'], coords={'x': ['x1', 'x2']})
   >>> _id, _ = xdb.put(a)
   >>> xdb.get(_id)

   <xarray.DataArray (x: 2)>
   array([1, 2])
   Coordinates:
     * x        (x) <U2 'x1' 'x2'

Dask support:

.. code-block:: python

   >>> _id, future = xdb.put(a.chunk(1))  # store metadata and numpy variables
   >>> future.compute()  # store dask variables
   >>> b = xdb.get(_id)  # retrieve metadata and numpy variables
   >>> b

   <xarray.DataArray (x: 2)>
   dask.array<shape=(2,), dtype=int64, chunksize=(1,)>
   Coordinates:
     * x        (x) <U2 'x1' 'x2'

   >>> b.compute()  # retrieve dask variables

   <xarray.DataArray (x: 2)>
   array([1, 2])
   Coordinates:
     * x        (x) <U2 'x1' 'x2'


Index
-----
.. toctree::

   installing
   whats-new
   api_reference
   db_reference
   nep18


License
-------
.. image::
   _static/amphora.png

xarray_mongodb is developed by `Amphora <http://www.amphora.net/>`_ and is available
under the open source
`Apache License <http://www.apache.org/licenses/LICENSE-2.0.html>`_

The database storage specifications are patent-free and in the public domain.
Anybody can write an alternative implementation; compatibility with the Python
module is not enforced by law, but strongly encouraged.
