Developer notes: Pint and Sparse
================================
.. note::
   This page is for people contributing patches to the xarray_mongodb library itself.

   If you just want to use `Pint <https://pint.readthedocs.io/>`_ or
   `Sparse <https://sparse.pydata.org/>`_, just make sure you satisfy the dependencies
   (see :doc:`installing`) and feed the data through! Also read the documentation of the
   ``ureg`` parameter when initialising :class:`~xarray_mongodb.XarrayMongoDB`.

   For how pint and sparse objects are stored on the database, see :doc:`db_reference`.


What is NEP18, and how it impacts xarray_mongodb
------------------------------------------------
Several "numpy-like" libraries support a duck-type interface, specified in
`NEP18 <https://numpy.org/neps/nep-0018-array-function-protocol.html>`_, so that
both numpy and other NEP18-compatible libraries can transparently wrap around them.

xarray_mongodb does not, itself, use NEP18. However, it does explicitly support several
data types that are possible thanks to NEP18. Namely,

- A :class:`xarray.Variable` can directly wrap:

  - a :class:`numpy.ndarray`, or
  - a :class:`sparse.COO`, or
  - a :class:`pint.quantity._Quantity`, or
  - a :class:`dask.array.Array`.

  The wrapped object is accessible through the ``.data`` property.

  .. note::
     :class:`xarray.IndexVariable` wraps a :class:`pandas.Index`, but the ``.data``
     property converts it on the fly to a :class:`numpy.ndarray`.

- A :class:`dask.array.Array` can directly wrap:

  - a :class:`numpy.ndarray`, or
  - a :class:`sparse.COO`, or
  - a :class:`pint.quantity._Quantity`.

  The wrapped object cannot be accessed until the dask graph is computed; however the
  object meta-data is visible without computing through the ``._meta`` property.

- A :class:`pint.quantity._Quantity` can directly wrap:

  - a :class:`numpy.ndarray`, or
  - a :class:`sparse.COO`.

  .. note::
     Vanilla pint can also wrap int, float, :class:`decimal.Decimal`, or
     :class:`dask.array.Array`, but they are automatically transformed to
     :class:`numpy.ndarray` as soon as xarray wraps around the Quantity.

  The wrapped object is accessible through the ``.magnitude`` property.

- A :class:`sparse.COO` is always backed by two :class:`numpy.ndarray` objects,
  ``.data`` and ``.coords``.

Worst case
----------
The most complicated use case that xarray_mongodb has to deal with is

1. a :class:`xarray.Variable`, which wraps around
2. a :class:`dask.array.Array`, which wraps around
3. a :class:`pint.quantity._Quantity`, which wraps around
4. a :class:`sparse.COO`, which is built on top of
5. two :class:`numpy.ndarray`.

.. code::

   >>> import dask.array as da
   >>> import numpy as np
   >>> import pint
   >>> import sparse
   >>> import xarray
   >>> ureg = pint.UnitRegistry()
   >>> a = xarray.DataArray(
   ...     da.from_array(
   ...         ureg.Quantity(
   ...             sparse.COO.from_numpy(
   ...                 np.array([0, 0, 1.1])
   ...             ),
   ...             "kg"
   ...     ), asarray=False)
   ... )
   >>> a
   <xarray.DataArray (dim_0: 3)>
   dask.array<array, shape=(3,), dtype=float64, chunksize=(3,), chunktype=pint.Quantity>
   Dimensions without coordinates: dim_0
   >>> a.data
   dask.array<array, shape=(3,), dtype=float64, chunksize=(3,), chunktype=pint.Quantity>
   >>> a.data._meta
   <Quantity(<COO: shape=(0,), dtype=float64, nnz=0, fill_value=0.0>, 'kilogram')>
   >>> a.data._meta.units
   <Unit('kilogram')>
   >>> a.data._meta.magnitude
   <COO: shape=(0,), dtype=float64, nnz=0, fill_value=0.0>
   >>> a.data.compute()
   <Quantity(<COO: shape=(3,), dtype=float64, nnz=1, fill_value=0.0>, 'kilogram')>
   >>> a.data.compute().units
   <Unit('kilogram')>
   >>> a.data.compute().magnitude
   <COO: shape=(3,), dtype=float64, nnz=1, fill_value=0.0>
   >>> a.data.compute().magnitude.data
   array([1.1])
   >>> a.data.compute().magnitude.coords
   array([[2]])

Legacy support
--------------
xarray_mongodb has to cope with a few caveats with legacy versions of its dependencies:

- It requires numpy >= 1.13; however NEP18 was first introduced in version 1.16 and even
  after that it may be disabled through environment variables
- It requires dask >= 1.1; however the ``da.Array._meta`` property, which exposes
  wrapped non-numpy objects, was not added until version 2.0
- It requires xarray >= 0.10.4; however NEP18 support was first introduced in version
  1.13.
