import bson
import pytest
import xarray

from xarray_mongodb import DocumentNotFoundError, XarrayMongoDBAsyncIO

from . import assert_chunks_index, requires_motor
from .data import ds, expect_meta_minimal, parametrize_roundtrip


@requires_motor
def test_init(async_db):
    xdb = XarrayMongoDBAsyncIO(
        async_db, "foo", chunk_size_bytes=123, embed_threshold_bytes=456
    )
    assert xdb.meta.database is async_db
    assert xdb.meta.name == "foo.meta"
    assert xdb.chunks.database is async_db
    assert xdb.chunks.name == "foo.chunks"
    assert xdb.chunk_size_bytes == 123
    assert xdb.embed_threshold_bytes == 456


@requires_motor
@pytest.mark.asyncio
async def test_index_on_put(async_xdb):
    indices = await async_xdb.chunks.list_indexes().to_list(None)
    assert not indices
    await async_xdb.put(xarray.DataArray([1, 2]))
    indices = await async_xdb.chunks.list_indexes().to_list(None)
    assert_chunks_index(indices)


@requires_motor
@pytest.mark.asyncio
async def test_index_on_get(async_xdb):
    indices = await async_xdb.chunks.list_indexes().to_list(None)
    assert not indices
    _id = (
        await async_xdb.meta.insert_one(
            {"coords": {}, "data_vars": {}, "chunkSize": 261120}
        )
    ).inserted_id
    await async_xdb.get(_id)
    indices = await async_xdb.chunks.list_indexes().to_list(None)
    assert_chunks_index(indices)


@requires_motor
@pytest.mark.asyncio
@parametrize_roundtrip
@pytest.mark.parametrize("chunk_size_bytes", [16, 2 ** 20])
async def test_roundtrip(
    async_xdb,
    compute,
    load,
    chunks,
    embed_threshold_bytes,
    chunk_size_bytes,
):
    async_xdb.chunk_size_bytes = chunk_size_bytes
    async_xdb.embed_threshold_bytes = embed_threshold_bytes

    if compute:
        _id, future = await async_xdb.put(ds.compute())
        assert future is None
    else:
        _id, future = await async_xdb.put(ds)
        future.compute()
    assert isinstance(_id, bson.ObjectId)

    ds2 = await async_xdb.get(_id, load=load)
    xarray.testing.assert_identical(ds, ds2)
    assert {k: v.chunks for k, v in ds2.variables.items()} == chunks


@requires_motor
@pytest.mark.asyncio
async def test_minimal(async_xdb):
    ds_min = xarray.Dataset()
    _id, _ = await async_xdb.put(ds_min)
    assert await async_xdb.meta.find().to_list(None) == expect_meta_minimal(_id)
    assert await async_xdb.chunks.find({}, {"_id": False}).to_list(None) == []
    out = await async_xdb.get(_id)
    xarray.testing.assert_identical(ds_min, out)


@requires_motor
@pytest.mark.asyncio
async def test_meta_not_found(async_xdb):
    with pytest.raises(DocumentNotFoundError) as ex:
        await async_xdb.get(bson.ObjectId("deadbeefdeadbeefdeadbeef"))
    assert str(ex.value) == "deadbeefdeadbeefdeadbeef"
