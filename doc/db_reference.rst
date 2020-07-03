Database Reference
==================
xarray_mongodb stores data on MongoDB in a format that is
agnostic to Python; this allows writing clients in different languages.

Like with `GridFS <https://docs.mongodb.com/manual/core/gridfs/>`_, data is
split across two collections, ``<prefix>.meta`` and ``<prefix>.chunks``. By
default, these are ``xarray.meta`` and ``xarray.chunks``.

.. note::
   At the moment of writing, support for sparse arrays has not been implemented yet.


xarray.meta
-----------
The ``<prefix>.meta`` collection contains one document per
:class:`xarray.Dataset` or :class:`xarray.DataArray` object, formatted as
follows::

    {
        '_id': bson.ObjectId(...),
        'attrs': bson.SON(...)  (optional),
        'chunkSize': 261120,
        'coords': bson.SON(...),
        'data_vars': bson.SON(...),
        'name': '<str>' (optional),
    }

Where:

- ``_id`` is the unique ID of the xarray object
- ``attrs``, ``coords``, and ``data_vars`` are ``bson.SON`` objects with
  the same order as the dictionaries in the xarray object (note how dicts
  preserve insertion order starting from Python 3.6).
- ``attrs`` are the ``Dataset.attrs`` or ``DataArray.attrs``, in native MongoDB format.
  Python object types that are not recognized by PyMongo are not supported. Omit when no
  attrs are available.
- chunkSize is the number of bytes stored at most in each document in the
  ``<prefix>.chunks`` collection. This is not to be confused with dask chunk
  size; for each dask chunk there are one or more MongoDB documents in the
  ``<prefix>.chunks`` collection (see later).
- ``name`` is the ``DataArray.name``; omit for unnamed arrays and Datasets.
- ``coords`` and ``data_vars`` contain one key/value pair for every
  :class:`xarray.Variable`, where the key is the variable name and the value is a dict
  defined as follows::

    {
        'chunks': [[2, 2], [2, 2]],
        'dims': ['x'],
        'dtype': '<i8',
        'shape': [4, 4],
        'type': <'ndarray'|'COO'>,
        'attrs': bson.SON(...) (optional),
        'units': <str> (optional),

        # For ndarray only; omit in case of sparse.COO
        'data': <bytes> (optional),

        # For sparse.COO only; omit in case of ndarray
        'fill_value': <bytes> (optional),
        'sparse_data': <bytes> (optional),
        'sparse_coords': <bytes> (optional),
        'nnz': <int> (optional),
     }

  - ``chunks`` are the dask chunk sizes at the moment of storing the array, or None if
    the variable was not backed by dask at the moment of storing the object.
  - ``dims`` are the names of the variable dimensions
  - ``dtype`` is the dtype of the numpy/dask variable, always in string format
  - ``shape`` is the overall shape of the numpy/dask array
  - ``type`` is the backend array type; ``ndarray`` for dense objects and ``COO``
    for :class:`sparse.COO` objects.
  - ``attrs`` are the variable attributes, if any
  - ``units`` is the string representation of :class:`pint.Unit`, e.g.
    ``kg * m /s ** 2``. The exact meaning of each symbol is deliberately omitted here
    and remitted to pint (or whatever other engine is used to handle units of measures).
    Omit for unit-less objects.
  - ``data`` contains the raw numpy buffer of the variable in the metadata document. It
    is meant to be used for small variables only. The buffer is in row-major (C) order
    and little endian encoding. If ``data`` is defined, ``type`` must be set to
    ndarray, ``chunks`` must always be None, and there must not be any documents for
    the variable in the ``<prefix>.chunks`` collection.
  - ``fill_value`` is the default value of a sparse array.
    It is a bytes buffer in little endian encoding of as many bytes as implied by dtype.
    This format allows encoding dtypes that are not native to MongoDB, e.g. complex
    numbers. Never present when type=ndarray.
  - ``sparse_data``, ``sparse_coords`` and ``nnz`` store embedded sparse arrays.
    See `sparse_arrays`_.

:class:`xarray.DataArray` objects are identifiable by having exactly one variable in
``data_vars``, conventionally named ``__DataArray__``. Note how ``DataArray.attrs`` are
the same as the attributes of its data variable; in xarray_mongodb they are only stored
in the top-level ``attrs`` key (there is never a ``data_vars.__DataArray__.attrs`` key).

.. note::
   When dealing with dask variables, ``shape`` and/or ``chunks`` may contain NaN instead
   of integer sizes when the variable size is unknown at the moment of graph definition.
   Also, ``dtype``, ``type``, and ``fill_value`` may potentially be wrong in the
   ``meta`` document and may be overridden by the ``chunks`` documents (see below).


xarray.chunks
-------------
The ``<prefix>.chunks`` collection contains the numpy data underlying the
array. There is a N:1 relationship between the chunks and the meta documents.

Each document is formatted as follows::

        {
            '_id': bson.ObjectId(...),
            'meta_id': bson.ObjectId(...),
            'name': 'variable name',
            'chunk': [0, 0],
            'dtype': '<i8',
            'shape': [1, 2]},
            'n': 0,
            'type': <'ndarray'|'COO'>,

            # For ndarray only; omit in case of sparse.COO
            'data': <bytes>,

            # For sparse.COO only; omit in case of ndarray
            'sparse_data': <bytes>,
            'sparse_coords': <bytes>',
            'nnz': <int>,
            'fill_value': <bytes>,
        }

Where:

- ``meta_id`` is the Object Id of the ``<prefix>.meta`` collection
- ``name`` is the variable name, matching one defined in ``<prefix>.meta``
- ``chunk`` is the dask chunk ID, or None for variables that were not backed by dask at
  the moment of storing the object
- ``dtype`` is the numpy dtype. It may be mismatched with, and overrides, the
  one defined in the ``meta`` collection.
- ``shape`` is the size of the current chunk. Unlike the ``shape`` and
  ``chunks`` variables defined in ``<prefix>.meta``, it is never NaN.
- ``n`` is the sequential document counter for the current variable and
  chunk (see below)
- ``type`` is the raw array type; ``ndarray`` for dense arrays; ``COO`` for sparse ones.
  It may be mismatched with, and overrides, the one defined in the ``meta`` collection.
- ``data`` is the raw numpy buffer, in row-major (C) order and little endian encoding.

Since numpy arrays and dask chunks can be larger than the maximum size a MongoDB
document can hold (typically 16MB), each numpy array or dask chunk may be split across
multiple documents, much like it happens in GridFS.
If the number of bytes in ``data`` would be larger than ``chunkSize``, then it is split
across multiple documents, with n=0, n=1, ... etc. The split happens after converting
the numpy array into a raw bytes buffer, and may result in having numpy points split
across different documents if ``chunkSize`` is not an exact multiple of the
``dtype`` size.

.. note::
   It is possible for all variables to be embedded into the metadata. In such a case,
   there won't be any documents in the chunks collection.


.. _sparse_arrays:

Sparse arrays
-------------
Sparse arrays (constructed using the Python class :class:`sparse.COO`) differ from
dense arrays as follows:

- In ``xarray.meta``,

  - The ``type`` field has value ``COO``
  - Extra field ``fill_value`` contains the value for all cells that are not explicitly
    listed. It is a raw binary blob in little endian encoding containing exactly one
    element of the indicated dtype.

- In ``xarray.chunks``,

  - The ``type`` field has value ``COO``
  - Extra field ``fill_value`` contains the value for all cells that are not
    explicitly listed
  - Extra field ``nnz`` is a non-negative integer (possibly zero) counting the number of
    cells that differ from ``fill_value``.
  - There is no ``data`` field.
  - The ``sparse_data`` field contains sparse values. It is a binary blob representing a
    one-dimensional numpy array of the indicated dtype with as many elements as ``nnz``.
  - The field ``sparse_coords`` is a binary blob representing a two-dimensional numpy
    array, with as many rows as the number of dimensions (see ``shape``) and as many
    columns as ``nnz``. It always contains unsigned integers in little endian format,
    regardless of the declared dtype. The word length is:

    - If max(shape) < 256, 1 byte
    - If 256 <= max(shape) < 2**16, 2 bytes
    - If 2**16 <= max(shape) < 2**32, 4 bytes
    - Otherwise, 8 bytes

    Each column of ``sparse_coords`` indicates the coordinates of the matching value in
    ``sparse_data``.

See next section for examples.

When the total of the ``sparse_data`` and ``sparse_coords`` bytes exceeds ``chunkSize``,
then the information is split across multiple documents, as follows:

1. Documents containing slices of ``sparse_data``; in all but the last one,
   ``sparse_coords`` is a bytes object of size 0
2. Documents containing slices of ``sparse_coords``; in all but the first one,
   ``sparse_data`` is a bytes object of size 0

.. note::
   When nnz=0, both data and coords are bytes objects of size 0.


Examples
--------
xarray object::

    xarray.Dataset(
        {"x": [[0, 1.1,   0],
               [0,   0, 2.2]]
        }
    )

chunks document (dense)::

    {
        '_id': bson.ObjectId(...),
        'meta_id': bson.ObjectId(...),
        'name': 'x',
        'chunk': [0, 0],
        'dtype': '<f8',
        'shape': [2, 3],
        'n': 0,
        'type': 'ndarray',
        'data': # 48 bytes buffer that contains [0, 1.1, 0, 0, 0, 2.2]
    }

chunks document (sparse)::

    {
        '_id': bson.ObjectId(...),
        'meta_id': bson.ObjectId(...),
        'name': 'x',
        'chunk': [0, 0],
        'dtype': '<f8',
        'shape': [2, 3]},
        'n': 0,
        'type': 'COO',
        'nnz': 2,
        'fill_value': b'\x00\x00\x00\x00\x00\x00\x00\x00',
        'sparse_data': # 16 bytes buffer that contains [1.1, 2.2]
        'sparse_coords': # 4 bytes buffer that contains [[0, 1,
                         #                               [1, 2]]
    }

Indexing
--------
Documents in ``<prefix>.chunks`` are identifiable by a unique functional key
``(meta_id, name, chunk, n)``. The driver automatically creates a non-unique index
``(meta_id, name, chunk)`` on the collection. Indexing ``n`` is unnecessary as
all the segments for a chunk are always read back together.


Missing data
------------
``<prefix>.chunks`` may miss some or all of the documents needed to
reconstruct the xarray object. This typically happens when:

- the user invokes ``put()``, but then does not compute the returned future
- some or all of the dask chunks fail to compute because of a fault at any
  point upstream in the dask graph
- there is a fault in MongoDB, e.g. the database becomes unreachable
  between the moment ``put()`` is invoked and the moment the future is
  computed, or if the disk becomes full.

The document in ``<prefix>.meta`` allows defining the
``(meta_id, name, chunk)`` search key for all objects in ``<prefix>.chunks``
and identify any missing documents. When a chunk is split across multiple
documents, one can figure out if the retrieved documents (n=0, n=1, ...) are
the complete set:

- for dense arrays (type=ndarray), the number of bytes in ``data`` must be the same as
  the productory of ``shape`` multiplied by ``dtype``.size.
- for sparse arrays(type=COO), the number of bytes in ``data`` plus ``coords`` must be
  the same as ``nnz * (dtype.size + len(shape) * coords.dtype.size)`` where
  ``coords.dtype.size`` is either 1, 2, 4 or 8 depending on ``max(shape)`` (see above).
