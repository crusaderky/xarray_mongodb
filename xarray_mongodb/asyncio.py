"""asyncio driver based on Motor
"""
import asyncio
from functools import wraps
from typing import Callable, Collection, Optional, Tuple, Union

import bson
import motor.motor_asyncio
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


def _create_index(func: Callable) -> Callable:
    """Asynchronous decorator function that create the index on the 'chunk' collection
    on the first get() or put()
    """

    @wraps(func)
    async def wrapper(self: "XarrayMongoDBAsyncIO", *args, **kwargs):
        if not self._has_index:
            idx_fut = asyncio.ensure_future(
                self.chunks.create_index(CHUNKS_INDEX, background=True)
            )
        try:
            return await func(self, *args, **kwargs)
        finally:
            if not self._has_index:
                await idx_fut
                self._has_index = True

    return wrapper


class XarrayMongoDBAsyncIO(XarrayMongoDBCommon):
    """:mod:`asyncio` driver for MongoDB to read/write xarray objects

    :param database:
        :class:`motor.motor_asyncio.AsyncIOMotorDatabase`
    :param str collection:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    :param int chunk_size_bytes:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    :param int embed_threshold_bytes:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    :param pint.registry.UnitRegistry ureg:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    """

    # This method is just for overriding the typing annotation of database
    def __init__(
        self,
        database: motor.motor_asyncio.AsyncIOMotorDatabase,
        collection: str = "xarray",
        *,
        chunk_size_bytes: int = CHUNK_SIZE_BYTES_DEFAULT,
        embed_threshold_bytes: int = EMBED_THRESHOLD_BYTES_DEFAULT,
        ureg: UnitRegistry = None,
    ):
        XarrayMongoDBCommon.__init__(**locals())

    @_create_index
    async def put(
        self, x: Union[xarray.DataArray, xarray.Dataset]
    ) -> Tuple[bson.ObjectId, Optional[Delayed]]:
        """Asynchronous variant of :meth:`xarray_mongodb.XarrayMongoDB.put`"""
        meta, variables_data = self._dataset_to_meta(x)
        _id = (await self.meta.insert_one(meta)).inserted_id
        chunks, delayed = self._dataset_to_chunks(variables_data, _id)
        if chunks:
            await self.chunks.insert_many(chunks)
        return _id, delayed

    @_create_index
    async def get(
        self, _id: bson.ObjectId, load: Union[bool, None, Collection[str]] = None
    ) -> Union[xarray.DataArray, xarray.Dataset]:
        """Asynchronous variant of :meth:`xarray_mongodb.XarrayMongoDB.get`"""
        meta = await self.meta.find_one({"_id": _id})
        if not meta:
            raise DocumentNotFoundError(_id)
        load_norm, chunks_query = self._prepare_get(meta, load)
        if chunks_query:
            chunks = await self.chunks.find(chunks_query).to_list(None)
        else:
            chunks = []
        return self._docs_to_dataset(meta, chunks, load_norm)
