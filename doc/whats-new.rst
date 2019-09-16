.. currentmodule:: xarray_mongodb

What's New
==========

.. _whats-new.0.1.1:

v0.1.1 (unreleased)
-------------------
- **Database structure change**: removed 'units' field from ``xarray.chunks`` collection.
  pint must always wrap around dask, and not the other way around. Note that, at the moment
  of writing, xarray->pint->dask is broken upstream.


.. _whats-new.0.1.0:

v0.1.0 (2019-03-13)
-------------------

Initial release.
