Installation
============

.. _dependencies:

Required dependencies
---------------------
- Python 3.9 or later
- MongoDB 4.4 or later
- `xarray <http://xarray.pydata.org>`_
- `dask <https://dask.org/>`_
- `PyMongo <https://pymongo.readthedocs.io/en/stable/>`_


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
- **all other libraries:** 12 months

You can see the actual minimum supported and tested versions here:
`<https://github.com/crusaderky/xarray_mongodb/blob/main/ci/requirements-min-all-deps.yml>`_


.. _build_sphinx:

Sphinx documentation
--------------------
To build the Sphinx documentation:

1. Source conda environment
2. Move to the root directory of this project
3. Execute::

     conda env create -n xarray_mongodb_docs --file ci/requirements-docs.yml
     conda activate xarray_mongodb_docs
     sphinx-build -n -j auto -b html -d build/doctrees doc build/html


.. _run_tests:

Testing
-------
To run the test suite:

1. Start MongoDB on localhost (no password)
2. Source conda environment
3. Move to the root directory of this project
4. Execute::

     conda env create -n xarray_mongodb --file ci/requirements-latest.yml
     conda activate xarray_mongodb
     py.test
