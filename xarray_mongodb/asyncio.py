"""asyncio driver based on Motor
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection
from functools import wraps
from typing import TYPE_CHECKING, TypeVar

import bson
import motor.motor_asyncio
import xarray
from dask.delayed import Delayed

from xarray_mongodb.common import (
    CHUNK_SIZE_BYTES_DEFAULT,
    CHUNKS_INDEX,
    EMBED_THRESHOLD_BYTES_DEFAULT,
    XarrayMongoDBCommon,
)
from xarray_mongodb.compat import UnitRegistry
from xarray_mongodb.errors import DocumentNotFoundError

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import Concatenate, ParamSpec

    P = ParamSpec("P")
    T = TypeVar("T")


def _create_index(
    func: Callable[Concatenate[XarrayMongoDBAsyncIO, P], Awaitable[T]]
) -> Callable[Concatenate[XarrayMongoDBAsyncIO, P], Awaitable[T]]:
    """Asynchronous decorator function that create the index on the 'chunk' collection
    on the first get() or put()
    """

    @wraps(func)
    async def wrapper(
        self: XarrayMongoDBAsyncIO, *args: P.args, **kwargs: P.kwargs
    ) -> T:
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
        ureg: UnitRegistry | None = None,
    ):
        XarrayMongoDBCommon.__init__(**locals())

    @_create_index
    async def put(
        self, x: xarray.DataArray | xarray.Dataset
    ) -> tuple[bson.ObjectId, Delayed | None]:
        """Asynchronous variant of :meth:`xarray_mongodb.XarrayMongoDB.put`"""
        meta, variables_data = self._dataset_to_meta(x)
        _id = (await self.meta.insert_one(meta)).inserted_id
        chunks, delayed = self._dataset_to_chunks(variables_data, _id)
        if chunks:
            await self.chunks.insert_many(chunks)
        return _id, delayed

    @_create_index
    async def get(
        self, _id: bson.ObjectId, load: bool | Collection[str] | None = None
    ) -> xarray.DataArray | xarray.Dataset:
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
