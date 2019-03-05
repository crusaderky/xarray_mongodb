.. _installing:

Installation
============

Required dependencies
---------------------

- Python 3.5 or later
- `xarray <http://xarray.pydata.org>`_
- `dask <https://dask.org/>`_
- `PyMongo <https://api.mongodb.com/python/current/>`_


Optional dependencies
---------------------
For asyncio support:

 - `Motor <https://motor.readthedocs.io//>`_


Testing
-------

To run the test suite after installing xarray_mongodb,
first install (via pypi or conda)

- `py.test <https://pytest.org>`__
- `pytest-asyncio <https://github.com/pytest-dev/pytest-asyncio>`_
  (only needed if motor is installed)

and run
``py.test --pyargs xarray_mongodb``.

