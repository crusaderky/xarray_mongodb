"""asyncio driver based on Motor
"""
import asyncio
from typing import Collection, Optional, Tuple, Union

import bson
import motor.motor_asyncio
import xarray
from dask.delayed import Delayed

from .common import CHUNK_SIZE_BYTES_DEFAULT, CHUNKS_INDEX, XarrayMongoDBCommon
from .errors import DocumentNotFoundError
from .nep18 import UnitRegistry


class XarrayMongoDBAsyncIO(XarrayMongoDBCommon):
    """:mod:`asyncio` driver for MongoDB to read/write xarray objects

    :param database:
        :class:`motor.motor_asyncio.AsyncIOMotorDatabase`
    :param str collection:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    :param int chunk_size_bytes:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    :param pint.registry.UnitRegistry ureg:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    """

    # This method is just for overriding the typing annotation of database
    def __init__(
        self,
        database: motor.motor_asyncio.AsyncIOMotorDatabase,
        collection: str = "xarray",
        chunk_size_bytes: int = CHUNK_SIZE_BYTES_DEFAULT,
        ureg: UnitRegistry = None,
    ):
        super().__init__(database, collection, chunk_size_bytes, ureg)

    async def _create_index(self) -> None:
        """Create the index on the 'chunk' collection
        on the first get() or put()
        """
        if not self._has_index:
            await self.chunks.create_index(CHUNKS_INDEX, background=True)
            self._has_index = True

    async def put(
        self, x: Union[xarray.DataArray, xarray.Dataset]
    ) -> Tuple[bson.ObjectId, Optional[Delayed]]:
        """Asynchronous variant of :meth:`xarray_mongodb.XarrayMongoDB.put`
        """
        loop = asyncio.get_event_loop()
        index_task = loop.create_task(self._create_index())
        meta = self._dataset_to_meta(x)
        _id = (await self.meta.insert_one(meta)).inserted_id
        chunks, delayed = self._dataset_to_chunks(x, _id)
        if chunks:
            await self.chunks.insert_many(chunks)
        await index_task
        return _id, delayed

    async def get(
        self, _id: bson.ObjectId, load: Union[bool, None, Collection[str]] = None
    ) -> Union[xarray.DataArray, xarray.Dataset]:
        """Asynchronous variant of :meth:`xarray_mongodb.XarrayMongoDB.get`
        """
        loop = asyncio.get_event_loop()
        index_task = loop.create_task(self._create_index())
        meta = await self.meta.find_one({"_id": _id})
        if not meta:
            await index_task
            raise DocumentNotFoundError(_id)
        load_norm = self._normalize_load(meta, load)
        chunks_query = self._chunks_query(meta, load_norm)

        chunks = await self.chunks.find(chunks_query).to_list(None)
        await index_task
        return self._docs_to_dataset(meta, chunks, load_norm)
