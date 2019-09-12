Installation
============

.. _dependencies:

Required dependencies
---------------------
- Python 3.6 or later
- `xarray <http://xarray.pydata.org>`_
- `dask <https://dask.org/>`_
- `PyMongo <https://api.mongodb.com/python/current/>`_


Optional dependencies
---------------------
- `Motor <https://motor.readthedocs.io//>`_ for asyncio support
- `Pint <https://pint.readthedocs.io/en/0.9/>`_
- `Sparse <https://sparse.pydata.org/en/latest/>`_ *(support not yet implemented)*

.. note::
   Pint and Sparse require:

   - numpy =1.16 and the environment variable ``NUMPY_EXPERIMENTAL_ARRAY_FUNCTION=1``,
     or numpy >=1.17
   - xarray >=0.13
   - dask >= 2.0


.. _build_sphinx:

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


.. _run_tests:

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
