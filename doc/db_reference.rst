Database Reference
==================
xarray_mongodb stores data on MongoDB in a format that is
agnostic to Python; this allows writing clients in different languages.

Like with `GridFS <https://docs.mongodb.com/manual/core/gridfs/>`_, data is
split across two collections, ``<prefix>.meta`` and ``<prefix>.chunks``. By
default, these are ``xarray.meta`` and ``xarray.chunks``.


xarray.meta
-----------
The ``<prefix>.meta`` collection contains one document per
:class:`xarray.Dataset` or :class:`xarray.DataArray` object, formatted as
follows::

    {
        '_id': bson.ObjectId(...),
        'attrs': bson.SON(...),
        'chunkSize': 261120,
        'coords': bson.SON(...),
        'data_vars': bson.SON(...),
        'name': '<DataArray.name>',
    }

Where:

- ``_id`` is the unique ID of the xarray object
- ``attrs``, ``coords``, and ``data_vars`` are ``bson.SON`` objects with
  the same order as the :class:`collections.OrderedDict` objects in the
  xarray object.
- ``attrs`` are the ``Dataset.attrs``, in native MongoDB format. Python object
  types that are not recognized by PyMongo are not supported. When storing a
  :class:`xarray.DataArray`, they're always an empty dictionary.
- chunkSize is the number of bytes stored at most in each document in the
  ``<prefix>.chunks`` collection. This is not to be confused with dask chunk
  size; for each dask chunk there are one or more MongoDB documents in the
  ``<prefix>.chunks`` collection (see later).
- ``name`` is the ``DataArray.name``, or None for unnamed arrays and Datasets.
- ``coords`` and ``data_vars`` contain one key/value pair for every
  xarray variable, where the key is the variable name and the value is a dict
  defined as follows::

    {
        'chunks': [[2, 2], [2, 2]],
        'dims': ['x'],
        'dtype': '<i8',
        'shape': [4, 4]
     }

  - ``chunks`` are the dask chunk sizes at the moment of storing the array,
    or None if the variable was not backed by dask.
  - ``dims`` are the names of the variable dimensions
  - ``dtype`` is the dtype of the numpy/dask variable, always in string format
  - ``shape`` is the overall shape of the numpy/dask array

  :class:`xarray.DataArray` objects are identifiable by having exactly one
  variable in ``data_vars``, named ``__DataArray__``.

.. note::
   When dealing with dask variables, both chunks and shape may contain NaN
   instead of integer sizes when the variable size is unknown at the moment of
   graph definition. Likewise, the dtype may be potentially wrong.


xarray.chunks
-------------
The ``<prefix>.chunks`` collection contains the numpy data underlying the
array. There is a N:1 relationship between the chunks and the meta documents.

Each document is formatted as follows::

        {
            '_id': bson.ObjectId(...),
            'meta_id': bson.ObjectId(...),
            'chunk': [0, 0],
            'dtype': '<i8',
            'name': 'variable name',
            'shape': [1, 2]},
            'n': 0,
            'data': <binary blob>,
        }

Where:

- ``meta_id`` is the Object Id of the ``<prefix>.meta`` collection
- ``chunk`` is the chunk ID, or None for variables that were backed by numpy
  at the moment of storing the object
- ``dtype`` is the numpy dtype. It may be mismatched with, and overrides, the
  one defined in the ``meta`` collection.
- ``name`` is the variable name, matching the one defined in ``<prefix>.meta``
- ``shape`` is the size of the current chunk. Unlike the ``shape`` and
  ``chunks`` variables defined in ``<prefix>.meta``, this is never NaN.
- ``n`` is the sequential document counter for the current variable and
  chunk (see below)
- ``data`` is the raw numpy buffer, in little endian encoding.


Since numpy arrays and dask chunks can be larger than the maximum size a
MongoDB document can hold (typically 16MB), each numpy array or dask chunk
may be split across multiple documents, much like in the GridFS chunks
colleciton.
If ``data`` would result to be larger than ``chunkSize``, then it is split
across multiple documents, with n=0, n=1, ... etc. The split happens after
converting the numpy array into a raw bytes buffer, and may result in having
numpy points split across different documents if ``chunkSize`` is not an exact
multiple of the ``dtype``.size.


Indexing
--------
Documents in ``<prefix>.chunks`` are identifiable by a unique key
``(meta_id, name, chunk, n)``. The driver automatically creates an index
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
the complete set by comparing the total size of the ``data`` with the
productory of ``size`` multiplied by ``dtype``.size.
