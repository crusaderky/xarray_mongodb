.. currentmodule:: xarray_mongodb

What's New
==========

.. _whats-new.0.2.0:

v0.2.0 (unreleased)
-------------------
- **Database structure change**: removed 'units' field from ``xarray.chunks``
  collection. pint must always wrap around dask, and not the other way around. Note
  that, at the moment of writing, xarray->pint->dask is broken upstream.
- xarray_mongodb now adopts a rolling :ref:`mindeps_policy` policy based on
  `NEP-29 <https://numpy.org/neps/nep-0029-deprecation_policy.html>`_.

  Increased minimum dependency versions:

  ======= ====== ====
  Package old    new
  ======= ====== ====
  xarray  0.10.4 0.11
  numpy   1.13   1.14
  pandas  0.21   0.24
  ======= ====== ====

- Added support for Python 3.8
- Added support for Motor on Windows (requires Motor >=2.1)
- toolz is now marked as a mandatory dependency


.. _whats-new.0.1.0:

v0.1.0 (2019-03-13)
-------------------

Initial release.
