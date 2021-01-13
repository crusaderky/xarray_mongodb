.. currentmodule:: xarray_mongodb

What's New
**********

.. _whats-new.0.3.0:

v0.2.1 (2021-01-13)
===================
- Support for dask 2020.12
- CI tests for MongoDB 4.4 and Python 3.9
- Use Sphinx 3 for documentation


.. _whats-new.0.2.0:

v0.2.0 (2020-07-03)
===================

Database structure changes
--------------------------
- Removed ``units`` field from the ``xarray.chunks`` collection. Pint must always wrap
  around dask, and not the other way around.
- ``DataArray.attrs`` was previously lost upon storage; it is now saved in the top-level
  ``attrs`` dict. (:issue:`10`).
- The ``attrs`` dict is now omitted if empty.
- Added ``attrs`` dict under every element of coords and data_vars (omitted if empty).
- Embed small variables into the metadata document. Added optional ``data`` key
  to each variable on the database. Added new parameter ``embed_threshold_bytes`` to
  control how aggressive embedding should be (see :doc:`api_reference`).


Packaging changes
-----------------
- xarray_mongodb now adopts a rolling :ref:`mindeps_policy` policy based on
  `NEP-29 <https://numpy.org/neps/nep-0029-deprecation_policy.html>`_.

  Increased minimum dependency versions:

  ======= ====== ====
  Package old    new
  ======= ====== ====
  xarray  0.10.4 0.13
  numpy   1.13   1.15
  dask    1.1    1.2
  pandas  0.21   0.24
  ======= ====== ====

- Added support for Python 3.8
- Added support for Motor on Windows (requires Motor >=2.1)
- toolz is now automatically installed by ``pip install xarray_mongodb``
- Now using setuptools-scm for versioning
- Now using GitHub actions for CI
- Run all CI tests on MongoDB 3.6 and 4.2


Other changes
-------------
- Fixed error when a package importing xarray_mongodb runs ``mypy --strict``
- Automatically cast scalar numpy objects (e.g. float64) wrapped by pint.Quantity to
  scalar ndarrays upon insertion


.. _whats-new.0.1.0:

v0.1.0 (2019-03-13)
===================

Initial release.
