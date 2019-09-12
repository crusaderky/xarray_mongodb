import asyncio

import bson
import pytest
import xarray

from xarray_mongodb import DocumentNotFoundError

from . import requires_motor
from .data import ds, expect_chunks, expect_meta, parametrize_roundtrip
from .fixtures import async_xdb

xdb = pytest.fixture(async_xdb)


@pytest.yield_fixture()
def event_loop():
    """pytest-asyncio by default creates a new event loop or every new coroutine. This
    happens *after* the fixtures are applied, which causes the AsyncIOMotorClient to be
    attached to the wrong event loop
    """
    yield asyncio.get_event_loop()


@requires_motor
@pytest.mark.asyncio
@parametrize_roundtrip
async def test_roundtrip(event_loop, xdb, compute, load, chunks):
    if compute:
        _id, future = await xdb.put(ds.compute())
        assert future is None
    else:
        _id, future = await xdb.put(ds)
        future.compute()
    assert isinstance(_id, bson.ObjectId)

    ds2 = await xdb.get(_id, load=load)
    xarray.testing.assert_identical(ds, ds2)
    assert {k: v.chunks for k, v in ds2.variables.items()} == chunks


@requires_motor
@pytest.mark.asyncio
async def test_db_contents(event_loop, xdb):
    assert xdb.meta.name.endswith(".meta")
    assert xdb.chunks.name.endswith(".chunks")
    assert xdb.meta.name.split(".")[:-1] == xdb.chunks.name.split(".")[:-1]

    _id, future = await xdb.put(ds)
    future.compute()

    assert await xdb.meta.find().to_list(None) == expect_meta(_id)
    chunks = sorted(
        await xdb.chunks.find({}, {"_id": False}).to_list(None),
        key=lambda doc: (doc["name"], doc["chunk"]),
    )
    assert chunks == sorted(
        expect_chunks(_id), key=lambda doc: (doc["name"], doc["chunk"])
    )


@requires_motor
@pytest.mark.asyncio
@pytest.mark.parametrize("name", [None, "foo"])
async def test_dataarray(event_loop, xdb, name):
    a = xarray.DataArray([1, 2], dims=["x"], coords={"x": ["x1", "x2"]})
    _id, _ = await xdb.put(a)
    a2 = await xdb.get(_id)
    xarray.testing.assert_identical(a, a2)


@requires_motor
@pytest.mark.asyncio
async def test_multisegment(event_loop, xdb):
    xdb.chunk_size_bytes = 4
    _id, future = await xdb.put(ds)
    future.compute()
    assert await xdb.chunks.find_one({"n": 2})
    ds2 = await xdb.get(_id)
    xarray.testing.assert_identical(ds, ds2)


@requires_motor
@pytest.mark.asyncio
async def test_size_zero(xdb):
    a = xarray.DataArray([])
    _id, _ = await xdb.put(a)
    a2 = await xdb.get(_id)
    xarray.testing.assert_identical(a, a2)


@requires_motor
@pytest.mark.asyncio
async def test_nan_chunks(xdb):
    """Test the case where the metadata of a dask array can't know the
    chunk sizes, as they are defined at the moment of computing it.

    .. note::
       We're triggering a degenerate case where one of the chunks has size 0.
    """
    a = xarray.DataArray([1, 2, 3, 4]).chunk(2)

    # two xarray bugs at the moment of writing:
    # https://github.com/pydata/xarray/issues/2801
    # a = a[a > 2]
    a = xarray.DataArray(a.data[a.data > 2])

    assert str(a.shape) == "(nan,)"
    assert str(a.chunks) == "((nan, nan),)"
    _id, future = await xdb.put(a)
    future.compute()
    a2 = await xdb.get(_id)
    assert str(a2.shape) == "(nan,)"
    assert str(a2.chunks) == "((nan, nan),)"

    # second xarray bug
    # xarray.testing.assert_identical(a, a2)
    xarray.testing.assert_identical(
        xarray.DataArray(a.data.compute()), xarray.DataArray(a2.data.compute())
    )


@requires_motor
@pytest.mark.asyncio
async def test_meta_not_found(event_loop, xdb):
    with pytest.raises(DocumentNotFoundError) as ex:
        await xdb.get(bson.ObjectId("deadbeefdeadbeefdeadbeef"))
    assert str(ex.value) == "deadbeefdeadbeefdeadbeef"


@requires_motor
@pytest.mark.asyncio
async def test_no_segments_found(event_loop, xdb):
    _id, future = await xdb.put(ds)
    future.compute()
    await xdb.chunks.delete_many({"name": "d", "chunk": [1, 0]})
    ds2 = await xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}"
    )


@requires_motor
@pytest.mark.asyncio
# A missing chunk with chunk_size_bytes=2 causes np.frombuffer to crash with
# 'ValueError: buffer size must be a multiple of element size'.
# A missing chunk with chunk_size_bytes=8 causes ndarray.reshape to crash with
# 'ValueError: cannot reshape array of size 1 into shape (1,2)'.
@pytest.mark.parametrize("chunk_size_bytes", (2, 8))
async def test_some_segments_not_found(event_loop, xdb, chunk_size_bytes):
    xdb.chunk_size_bytes = chunk_size_bytes
    _id, future = await xdb.put(ds)
    future.compute()
    await xdb.chunks.delete_one({"name": "d", "chunk": [1, 0], "n": 1})
    ds2 = await xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}"
    )
