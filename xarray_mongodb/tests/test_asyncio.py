import uuid
import bson
import pytest
import xarray
from xarray_mongodb import DocumentNotFoundError
from . import requires_motor
from .data import ds, parametrize_roundtrip, expect_chunks, expect_meta


@pytest.yield_fixture()
def event_loop():
    """pytest-asyncio by default creates a new event loop or every new
    coroutine. This happens *after* the fixtures are applied, which causes the
    AsyncIOMotorClient to be attached to the wrong event loop
    """
    import asyncio
    yield asyncio.get_event_loop()


@pytest.fixture
def xdb():
    import motor.motor_asyncio
    from xarray_mongodb import XarrayMongoDBAsyncIO

    client = motor.motor_asyncio.AsyncIOMotorClient()
    dbname = 'test_xarray_mongodb-%s' % uuid.uuid4()
    yield XarrayMongoDBAsyncIO(client[dbname])
    client.drop_database(dbname)


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
    assert xdb.meta.name == 'xarray.arrays'
    assert xdb.chunks.name == 'xarray.chunks'

    _id, future = await xdb.put(ds)
    future.compute()

    assert await xdb.meta.find().to_list(None) == expect_meta(_id)
    chunks = sorted(
        await xdb.chunks.find({}, {'_id': False}).to_list(None),
        key=lambda doc: (doc['name'], doc['chunk']))
    assert chunks == sorted(
        expect_chunks(_id),
        key=lambda doc: (doc['name'], doc['chunk']))


@requires_motor
@pytest.mark.asyncio
async def test_dataarray(event_loop, xdb):
    a = xarray.DataArray([1, 2], dims=['x'], coords={'x': ['x1', 'x2']})
    _id, _ = await xdb.put(a)
    a2 = await xdb.get(_id)
    xarray.testing.assert_identical(a, a2)


@requires_motor
@pytest.mark.asyncio
async def test_multisegment(event_loop, xdb):
    xdb.chunk_size_bytes = 4
    _id, future = await xdb.put(ds)
    future.compute()
    assert await xdb.chunks.find_one({'n': 2})
    ds2 = await xdb.get(_id)
    xarray.testing.assert_identical(ds, ds2)


@requires_motor
@pytest.mark.asyncio
async def test_meta_not_found(event_loop, xdb):
    with pytest.raises(DocumentNotFoundError) as ex:
        await xdb.get(bson.ObjectId('deadbeefdeadbeefdeadbeef'))
    assert str(ex.value) == 'deadbeefdeadbeefdeadbeef'


@requires_motor
@pytest.mark.asyncio
async def test_no_segments_found(event_loop, xdb):
    _id, future = await xdb.put(ds)
    future.compute()
    await xdb.chunks.delete_many({'name': 'd', 'chunk': [1, 0]})
    ds2 = await xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}")


@requires_motor
@pytest.mark.asyncio
# A missing chunk with chunk_size_bytes=2 causes np.frombuffer to crash
# with 'ValueError: buffer size must be a multiple of element size'
# A missing chunk with chunk_size_bytes=8 causes ndarray.reshape
# to crash with 'ValueError: cannot reshape array of size 1 into shape (1,2)'
@pytest.mark.parametrize('chunk_size_bytes', (2, 8))
async def test_some_segments_not_found(event_loop, xdb, chunk_size_bytes):
    xdb.chunk_size_bytes = chunk_size_bytes
    _id, future = await xdb.put(ds)
    future.compute()
    await xdb.chunks.delete_one({'name': 'd', 'chunk': [1, 0], 'n': 1})
    ds2 = await xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}")
