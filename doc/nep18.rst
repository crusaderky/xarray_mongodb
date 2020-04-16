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
  - a :class:`pint.Quantity`, or
  - a :class:`sparse.COO`, or
  - a :class:`dask.array.Array`.

  The wrapped object is accessible through the ``.data`` property.

  .. note::
     :class:`xarray.IndexVariable` wraps a :class:`pandas.Index`, but the ``.data``
     property converts it on the fly to a :class:`numpy.ndarray`.

- A :class:`pint.Quantity` can directly wrap:

  - a :class:`numpy.ndarray`, or
  - a :class:`sparse.COO`, or
  - a :class:`dask.array.Array`.

  .. note::
     Vanilla pint can also wrap int, float, :class:`decimal.Decimal`, but they are
     automatically transformed to :class:`numpy.ndarray` as soon as xarray wraps around
     the Quantity.

  The wrapped object is accessible through the ``.magnitude`` property.

- A :class:`dask.array.Array` can directly wrap:

  - a :class:`numpy.ndarray`, or
  - a :class:`sparse.COO`.

  The wrapped object cannot be accessed until the dask graph is computed; however the
  object meta-data is visible without computing through the ``._meta`` property.

  .. note::
     dask wrapping pint, while theoretically possible due to how NEP18 works, is not
     supported.

- A :class:`sparse.COO` is always backed by two :class:`numpy.ndarray` objects,
  ``.data`` and ``.coords``.

Worst case
----------
The most complicated use case that xarray_mongodb has to deal with is

1. a :class:`xarray.Variable`, which wraps around
2. a :class:`pint.Quantity`, which wraps around
3. a :class:`dask.array.Array`, which wraps around
4. a :class:`sparse.COO`, which is built on top of
5. two :class:`numpy.ndarray`.

The order is always the one described above. Simpler use cases may remove any of the
intermediate layers; at the top there's always has a :class:`xarray.Variable` and at the
bottom the data is always stored by :class:`numpy.ndarray`.

.. note::
   At the moment of writing, the example below doesn't work; see
   `pint#878 <https://github.com/hgrecco/pint/issues/878>`_.

.. code::

   >>> import dask.array as da
   >>> import numpy as np
   >>> import pint
   >>> import sparse
   >>> import xarray
   >>> ureg = pint.UnitRegistry()
   >>> a = xarray.DataArray(
   ...     ureg.Quantity(
   ...         da.from_array(
   ...             sparse.COO.from_numpy(
   ...                 np.array([0, 0, 1.1])
   ...             )
   ...         ), "kg"
   ...     )
   ... )
   >>> a
   <xarray.DataArray (dim_0: 3)>
   dask.array<array, shape=(3,), dtype=float64, chunksize=(3,), chunktype=pint.Quantity>
   Dimensions without coordinates: dim_0
   >>> a.data
   <Quantity(<dask.array<array, shape=(3,), dtype=float64, chunksize=(3,),
              chunktype=COO>>, 'kilogram')>
   >>> a.data.magnitude
   <dask.array<array, shape=(3,), dtype=float64, chunksize=(3,), chunktype=COO>
   >>> a.data.units
   <Unit('kilogram')>
   >>> a.data.magnitude._meta
   <COO: shape=(0,), dtype=float64, nnz=0, fill_value=0.0>
   >>> a.data.magnitude.compute()
   <COO: shape=(3,), dtype=float64, nnz=1, fill_value=0.0>
   >>> a.data.magnitude.compute().data
   array([1.1])
   >>> a.data.magnitude.compute().coords
   array([[2]])

Legacy support
--------------
xarray_mongodb has to cope with a few caveats with legacy versions of its dependencies:

- It requires numpy >= 1.15; however NEP18 was first introduced in v1.16 and
  consolidated in v1.17.
- It requires dask >= 1.2; however the ``da.Array._meta`` property, which exposes
  wrapped non-numpy objects, was not added until v2.0.

Hence, there is a set of minimum required versions when pint and sparse are not
involved, and a different set of much more recent ones when they are.

See also: :ref:`mindeps_policy`.
