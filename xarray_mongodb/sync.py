"""synchronous driver based on PyMongo
"""
from typing import Union, Tuple, Sequence
from pymongo.database import Database
from xarray import DataArray, Dataset
from bson.objectid import ObjectId
from dask.delayed import Delayed
from .common import (XarrayMongoDBCommon,
                     CHUNK_SIZE_BYTES_DEFAULT, CHUNKS_INDEX, CHUNKS_PROJECT)
from .errors import DocumentNotFoundError


class XarrayMongoDB(XarrayMongoDBCommon):
    """Synchronous driver for MongoDB to read/write
    xarray objects

    :param db:
        :class:`pymongo.database.Database`
    :param str collection:
        prefix of the collections to store the xarray data.
        Two collections will actually be created,
        <collection>.meta and <collection>.chunks.
    :param int chunk_size_bytes:
        Size of the payload in a document in the chunks collection.
        Not to be confused with dask chunks.
    """

    def __init__(self, db: Database,
                 collection: str = 'xarray',
                 chunk_size_bytes: int = CHUNK_SIZE_BYTES_DEFAULT):
        super().__init__(db, collection, chunk_size_bytes)
        self.chunks.create_index(CHUNKS_INDEX)

    def put(self, x: Union[DataArray, Dataset]) -> Tuple[ObjectId, Delayed]:
        """Write an xarray object to MongoDB.
        Variables that are backed by dask are not computed;
        instead their insertion in the database is delayed.
        All other variables are immediately inserted.

        :param x:
            :class:`xarray.DataArray` or :class:`xarray.Dataset`
        :returns:
            Tuple of:

            - MongoDB _id of the inserted object
            - :class:`~dask.delayed.Delayed`,
              or None if none of the variables use dask.
        """
        meta = self._dataset_to_meta(x)
        _id = self.meta.insert_one(meta).inserted_id
        chunks, delayed = self._dataset_to_chunks(
            x, _id)
        self.chunks.insert_many(chunks)
        return _id, delayed

    def get(self, _id: ObjectId, load: Union[bool, None, Sequence[str]]
            ) -> Union[DataArray, Dataset]:
        """Read an xarray object back from MongoDB

        :param :class:`~bson.objectid.ObjectId` _id:
            MongoDB object ID, as returned by
            :meth:`~MongoXarray.put`
        :param load:
            Determines which variables to load immediately
            and which instead delay loading with dask. Must be one of:

            None
                Match whatever was stored with put()
            True
                Immediately load all variables into memory.
                dask chunk information, if any, will be discarded.
            False
                Only load index variables in memory; delay the
                loading of everything else with dask.
            sequence of str
                variable names that must be immediately loaded into
                memory. Regardless of this, indices will always be loaded.
        :returns:
            :class:`xarray.DataArray` or :class:`xarray.Dataset`
        :raises DocumentNotFoundError:
            id_ not found in the MongoDB 'meta' collection, or one or more
            chunks are missing in the 'chunks' collection.
            This error typically happens when:

            - documents were deleted from the database
            - the Delayed returned by put() was never computed
            - one or more chunks of the dask variables failed to
              compute at any point during the graph resolution

            If chunks loading is delayed with dask (see 'load' parameter),
            this exception may be raised at compute() time.

        It is possible to invoke get() before put() is computed, as long as

        - you pass load=None or load=False
        - the output of get() is computed after the output of put() is computed
        """
        meta = self.meta.find_one({'_id': _id})
        if not meta:
            raise DocumentNotFoundError(_id)
        load = self._normalize_load(meta, load)
        chunks_query = self._chunks_query(meta, load)
        chunks = list(self.chunks.find(
            chunks_query, CHUNKS_PROJECT))
        return self._docs_to_dataset(meta, chunks, load)
