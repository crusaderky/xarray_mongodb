"""asyncio driver based on Motor
"""
import asyncio
from typing import Union, Tuple, Sequence

from xarray import DataArray, Dataset
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson.objectid import ObjectId
from dask.delayed import Delayed
from .common import (XarrayMongoDBCommon,
                     CHUNK_SIZE_BYTES_DEFAULT, CHUNKS_INDEX, CHUNKS_PROJECT)
from .errors import DocumentNotFoundError


class XarrayMongoDBAsyncIO(XarrayMongoDBCommon):
    """:mod:`asyncio` driver for MongoDB to read/write
    xarray objects

    :param db:
        :class:`motor.motor_asyncio.AsyncIOMotorDatabase`
    :param str collection:
        See :class:`XarrayMongoDB`
    :param int chunk_size_bytes:
        See :class:`XarrayMongoDB`
    """
    def __init__(self, db: AsyncIOMotorDatabase,
                 collection: str = 'xarray',
                 chunk_size_bytes: int = CHUNK_SIZE_BYTES_DEFAULT):
        super().__init__(db, collection, chunk_size_bytes)
        self._has_index = False

    async def _create_index(self):
        """Create the index on the 'chunk' collection
        on the first get() or put()
        """
        if not self._has_index:
            await self.chunks.create_index(CHUNKS_INDEX)
            self._has_index = True

    async def put(self, x: Union[DataArray, Dataset]
                  ) -> Tuple[ObjectId, Delayed]:
        """See :meth:`XarrayMongoDB.put`
        """
        index_task = asyncio.create_task(self._create_index())
        meta = self._dataset_to_meta(x)
        _id = (await self.meta.insert_one(meta)).inserted_id
        chunks, delayed = self._dataset_to_chunks(x, _id)
        await index_task
        await self.chunks.insert_many(chunks)
        return _id, delayed

    async def get(self, _id: ObjectId, load: Union[bool, None, Sequence[str]]
                  ) -> Union[DataArray, Dataset]:
        """See :meth:`XarrayMongoDB.get`
        """
        index_task = asyncio.create_task(self._create_index())
        meta = await self.meta.find_one({'_id': _id})
        if not meta:
            await index_task
            raise DocumentNotFoundError(_id)
        load = self._normalize_load(meta, load)
        chunks_query = self._chunks_query(meta, load)
        await index_task
        chunks = await self.chunks.find(chunks_query, CHUNKS_PROJECT).tolist()
        return self._docs_to_dataset(meta, chunks, load)
