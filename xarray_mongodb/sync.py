"""synchronous driver based on PyMongo
"""
from typing import Collection, Optional, Tuple, Union

import bson
import pymongo.database
import xarray
from dask.delayed import Delayed

from .common import (
    CHUNK_SIZE_BYTES_DEFAULT,
    CHUNKS_INDEX,
    EMBED_THRESHOLD_BYTES_DEFAULT,
    XarrayMongoDBCommon,
)
from .errors import DocumentNotFoundError
from .nep18 import UnitRegistry


class XarrayMongoDB(XarrayMongoDBCommon):
    """Synchronous driver for MongoDB to read/write
    xarray objects

    :param database:
        :class:`pymongo.database.Database`
    :param str collection:
        prefix of the collections to store the xarray data. Two collections will
        actually be created, <collection>.meta and <collection>.chunks.
    :param int chunk_size_bytes:
        Size of the payload in a document in the chunks collection. Not to be confused
        with dask chunks. dask chunks that are larger than chunk_size_bytes will be
        transparently split across multiple MongoDB documents.
    :param int embed_threshold_bytes:
        Cumulative size of variable buffers that will be embedded into the metadata
        documents in <collection>.meta. Buffers that exceed the threshold (starting from
        the largest) will be stored into the chunks documents in <collection>.chunks.

        .. note::
           - Embedded variables ignore the ``load`` parameter of :meth:`get`
           - dask variables are never embedded, regardless of size
           - set ``embed_threshold_bytes=0`` to force all buffers to be saved to
             <collection>.chunks, witht he only exception of size zero non-dask
             variables
           - size zero non-dask variables are always embedded

    :param pint.registry.UnitRegistry ureg:
        pint registry to allow putting and getting arrays with units.
        If omitted, it defaults to the global registry defined with
        :func:`pint.set_application_registry`. If the global registry was never set, it
        defaults to a standard registry built with :file:`defaults_en.txt`.
    """

    def __init__(
        self,
        database: pymongo.database.Database,
        collection: str = "xarray",
        *,
        chunk_size_bytes: int = CHUNK_SIZE_BYTES_DEFAULT,
        embed_threshold_bytes: int = EMBED_THRESHOLD_BYTES_DEFAULT,
        ureg: UnitRegistry = None,
    ):
        XarrayMongoDBCommon.__init__(**locals())

    def _create_index(self) -> None:
        """Create the index on the 'chunk' collection
        on the first get() or put()
        """
        if not self._has_index:
            self.chunks.create_index(CHUNKS_INDEX, background=True)
            self._has_index = True

    def put(
        self, x: Union[xarray.DataArray, xarray.Dataset]
    ) -> Tuple[bson.ObjectId, Optional[Delayed]]:
        """Write an xarray object to MongoDB. Variables that are backed by dask are not
        computed; instead their insertion in the database is delayed. All other
        variables are immediately inserted.

        This method automatically creates an index on the 'chunks' collection if there
        isn't one yet.

        :param x:
            :class:`xarray.DataArray` or :class:`xarray.Dataset`
        :returns:
            Tuple of:

            - MongoDB _id of the inserted object
            - dask delayed object, or None if there are no variables using dask. It must
              be explicitly computed in order to fully store the Dataset/DataArray on
              the database.

        .. warning::
           The dask future contains access full credentials to the MongoDB server. This
           commands caution if one pickles it and stores it on disk, or if he sends it
           over the network e.g. through `dask distributed
           <https://distributed.dask.org/en/latest/>`_.
        """
        self._create_index()
        meta, variables_data = self._dataset_to_meta(x)
        _id = self.meta.insert_one(meta).inserted_id
        chunks, delayed = self._dataset_to_chunks(variables_data, _id)
        if chunks:
            self.chunks.insert_many(chunks)
        return _id, delayed

    def get(
        self, _id: bson.ObjectId, load: Union[bool, None, Collection[str]] = None
    ) -> Union[xarray.DataArray, xarray.Dataset]:
        """Read an xarray object back from MongoDB

        :param :class:`~bson.objectid.ObjectId` _id:
            MongoDB object ID, as returned by :meth:`put`

        :param load:
            Determines which variables to load immediately and which instead delay
            loading with dask. Must be one of:

            None (default)
                Match whatever was stored with put(), including chunk sizes
            True
                Immediately load all variables into memory.
                dask chunk information, if any, will be discarded.
            False
                Only load indices in memory; delay the loading of everything else with
                dask.
            collection of str
                variable names that must be immediately loaded into memory. Regardless
                of this, indices are always loaded. Non-existing variables are ignored.
                When retrieving a DataArray, you can target the data with the special
                hardcoded variable name ``__DataArray__``.

            .. note::
               Embedded variables (see ``embed_threshold_bytes``) are always loaded
               regardless of this flag.

        :returns:
            :class:`xarray.DataArray` or :class:`xarray.Dataset`, depending on what was
            stored with :meth:`put`

        :raises DocumentNotFoundError:
            _id not found in the MongoDB 'meta' collection, or one or more chunks are
            missing in the 'chunks' collection.
            This error typically happens when:

            - documents were deleted from the database
            - the Delayed returned by put() was never computed
            - one or more chunks of the dask variables failed to compute at any point
              during the graph resolution

            If chunks loading is delayed with dask (see 'load' parameter), this
            exception may be raised at compute() time.

        It is possible to invoke :meth:`get` before :meth:`put` is computed, as long as:

        - The ``pass`` parameter is valued None, False, or does not list any variables
          that were backed by dask during :meth:`put`
        - the output of :meth:`get` is computed after the output of :meth:`put` is
          computed

        .. warning::
           The dask graph (if any) underlying the returned xarray object contains full
           access credentials to the MongoDB server. This commands caution if one
           pickles it and stores it on disk, or if he sends it over the network e.g.
           through `dask distributed <https://distributed.dask.org/en/latest/>`_.
        """
        self._create_index()
        meta = self.meta.find_one({"_id": _id})
        if not meta:
            raise DocumentNotFoundError(_id)
        load_norm, chunks_query = self._prepare_get(meta, load)
        if chunks_query:
            chunks = list(self.chunks.find(chunks_query))
        else:
            chunks = []
        return self._docs_to_dataset(meta, chunks, load_norm)
