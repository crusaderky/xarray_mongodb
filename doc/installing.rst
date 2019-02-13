.. _installing:

Installation
============

Required dependencies
---------------------

- Python 3.5.2 or later
- `xarray <http://xarray.pydata.org>`_
- `dask <https://dask.org/>`_
- `pymongo <https://api.mongodb.com/python/current/>`_


Optional dependencies
---------------------
For asyncio support:

 - `motor <https://motor.readthedocs.io//>`_


Testing
-------

To run the test suite after installing xarray_mongodb,
first install (via pypi or conda)

- `py.test <https://pytest.org>`__: Simple unit testing library

and run
``py.test --pyargs xarray_mongodb``.

