Installation
============

.. _dependencies:

Required dependencies
---------------------
- Python 3.6 or later
- MongoDB 3.6 or later
- `xarray <http://xarray.pydata.org>`_
- `dask <https://dask.org/>`_
- `toolz <https://toolz.readthedocs.io/>`_
- `PyMongo <https://api.mongodb.com/python/current/>`_


Optional dependencies
---------------------
- `Motor <https://motor.readthedocs.io//>`_ for asyncio support
- `Pint <https://pint.readthedocs.io/en/0.9/>`_
- `Sparse <https://sparse.pydata.org/en/latest/>`_ *(support not yet implemented)*


.. _mindeps_policy:

Minimum dependency versions
---------------------------
xarray_mongodb adopts a rolling policy regarding the minimum supported versions of its
dependencies:

- **Python:** 42 months
  (`NEP-29 <https://numpy.org/neps/nep-0029-deprecation_policy.html>`_)
- **numpy:** 24 months
  (`NEP-29 <https://numpy.org/neps/nep-0029-deprecation_policy.html>`_)
- **pandas:** 12 months
- **pint and sparse:** very latest available versions only, until the technology based
  on `NEP-18 <https://numpy.org/neps/nep-0018-array-function-protocol.html>`_ will have
  matured. This extends to all other libraries as well when one wants to use pint or
  sparse.
- **all other libraries:** 6 months

You can see the actual minimum supported and tested versions:

- `For using pint and/or sparse
  <https://github.com/AmphoraInc/xarray_mongodb/blob/master/ci/requirements-py36-min-nep18.yml>`_
- `For everything else
  <https://github.com/AmphoraInc/xarray_mongodb/blob/master/ci/requirements-py36-min-all-deps.yml>`_


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
