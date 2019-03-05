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


Sphinx documentation
--------------------
To build the Sphinx documentation:

1. Source conda environment
2. Move to the root directory of this project
3. Execute::

     conda env create -n xarray_mongodb_docs --file ci/requirements-docs.yml
     conda activate xarray_mongodb_docs
     export PYTHONPATH=$PWD
     sphinx-build -n -j auto -b html -d build/doctrees doc build/html


Testing
-------
To run the test suite:

1. Start MongoDB on localhost (no password)
2. Source conda environment
3. Move to the root directory of this project
4. Execute::

     conda env create -n xarray_mongodb_py37 --file ci/requirements-py37.yml
     conda activate xarray_mongodb_py37
     export PYTHONPATH=$PWD
     py.test

Replace py37 with any of the environments available in the :file:`ci`
directory.
