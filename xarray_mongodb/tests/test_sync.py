import bson
import uuid
import pytest
import xarray
from xarray_mongodb import DocumentNotFoundError
from .data import ds, parametrize_roundtrip, expect_chunks, expect_meta


@pytest.fixture
def xdb():
    import pymongo
    from xarray_mongodb import XarrayMongoDB

    client = pymongo.MongoClient()
    dbname = 'test_xarray_mongodb-%s' % uuid.uuid4()
    yield XarrayMongoDB(client[dbname])
    client.drop_database(dbname)


@parametrize_roundtrip
def test_roundtrip(xdb, compute, load, chunks):
    if compute:
        _id, future = xdb.put(ds.compute())
        assert future is None
    else:
        _id, future = xdb.put(ds)
        future.compute()
    assert isinstance(_id, bson.ObjectId)

    ds2 = xdb.get(_id, load=load)
    xarray.testing.assert_identical(ds, ds2)
    assert {k: v.chunks for k, v in ds2.variables.items()} == chunks


def test_db_contents(xdb):
    assert xdb.meta.name == 'xarray.arrays'
    assert xdb.chunks.name == 'xarray.chunks'

    _id, future = xdb.put(ds)
    future.compute()

    assert list(xdb.meta.find()) == expect_meta(_id)
    chunks = sorted(
        xdb.chunks.find({}, {'_id': False}),
        key=lambda doc: (doc['name'], doc['chunk']))
    assert chunks == sorted(
        expect_chunks(_id),
        key=lambda doc: (doc['name'], doc['chunk']))


def test_dataarray(xdb):
    a = xarray.DataArray([1, 2], dims=['x'], coords={'x': ['x1', 'x2']})
    _id, _ = xdb.put(a)
    a2 = xdb.get(_id)
    xarray.testing.assert_identical(a, a2)


def test_multisegment(xdb):
    xdb.chunk_size_bytes = 4
    _id, future = xdb.put(ds)
    future.compute()
    assert xdb.chunks.find_one({'n': 2})
    ds2 = xdb.get(_id)
    xarray.testing.assert_identical(ds, ds2)


def test_meta_not_found(xdb):
    with pytest.raises(DocumentNotFoundError) as ex:
        xdb.get(bson.ObjectId('deadbeefdeadbeefdeadbeef'))
    assert str(ex.value) == 'deadbeefdeadbeefdeadbeef'


def test_no_segments_found(xdb):
    _id, future = xdb.put(ds)
    future.compute()
    xdb.chunks.delete_many({'name': 'd', 'chunk': [1, 0]})
    ds2 = xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}")


# A missing chunk with chunk_size_bytes=2 causes np.frombuffer to crash
# with 'ValueError: buffer size must be a multiple of element size'
# A missing chunk with chunk_size_bytes=8 causes ndarray.reshape
# to crash with 'ValueError: cannot reshape array of size 1 into shape (1,2)'
@pytest.mark.parametrize('chunk_size_bytes', (2, 8))
def test_some_segments_not_found(xdb, chunk_size_bytes):
    xdb.chunk_size_bytes = chunk_size_bytes
    _id, future = xdb.put(ds)
    future.compute()
    xdb.chunks.delete_one({'name': 'd', 'chunk': [1, 0], 'n': 1})
    ds2 = xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}")
