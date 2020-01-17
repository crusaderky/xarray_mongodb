import bson
import pytest
import xarray

from xarray_mongodb import DocumentNotFoundError

from .data import (
    da,
    ds,
    expect_da_chunks,
    expect_da_meta,
    expect_ds_chunks,
    expect_ds_meta,
    expect_meta_minimal,
    parametrize_roundtrip,
)


@parametrize_roundtrip
def test_roundtrip(sync_xdb, compute, load, chunks):
    if compute:
        _id, future = sync_xdb.put(ds.compute())
        assert future is None
    else:
        _id, future = sync_xdb.put(ds)
        future.compute()
    assert isinstance(_id, bson.ObjectId)

    ds2 = sync_xdb.get(_id, load=load)
    xarray.testing.assert_identical(ds, ds2)
    assert {k: v.chunks for k, v in ds2.variables.items()} == chunks


def test_db_contents(sync_xdb):
    assert sync_xdb.meta.name.endswith(".meta")
    assert sync_xdb.chunks.name.endswith(".chunks")
    assert sync_xdb.meta.name.split(".")[:-1] == sync_xdb.chunks.name.split(".")[:-1]

    _id, future = sync_xdb.put(ds)
    future.compute()

    assert list(sync_xdb.meta.find()) == expect_ds_meta(_id)
    chunks = sorted(
        sync_xdb.chunks.find({}, {"_id": False}),
        key=lambda doc: (doc["name"], doc["chunk"]),
    )
    assert chunks == sorted(
        expect_ds_chunks(_id), key=lambda doc: (doc["name"], doc["chunk"])
    )


def test_minimal(sync_xdb):
    ds_min = xarray.Dataset()
    _id, _ = sync_xdb.put(ds_min)
    assert list(sync_xdb.meta.find()) == expect_meta_minimal(_id)
    assert list(sync_xdb.chunks.find({}, {"_id": False})) == []
    out = sync_xdb.get(_id)
    xarray.testing.assert_identical(ds_min, out)


@pytest.mark.parametrize("name", [None, "foo"])
def test_dataarray(sync_xdb, name):
    da2 = da.copy()
    if name:
        da2.name = name
    _id, _ = sync_xdb.put(da2)

    assert list(sync_xdb.meta.find()) == expect_da_meta(_id, name)
    assert list(sync_xdb.chunks.find({}, {"_id": False})) == expect_da_chunks(_id)
    out = sync_xdb.get(_id)
    xarray.testing.assert_identical(da2, out)


def test_multisegment(sync_xdb):
    sync_xdb.chunk_size_bytes = 4
    _id, future = sync_xdb.put(ds)
    future.compute()
    assert sync_xdb.chunks.find_one({"n": 2})
    ds2 = sync_xdb.get(_id)
    xarray.testing.assert_identical(ds, ds2)


def test_size_zero(sync_xdb):
    a = xarray.DataArray([])
    _id, _ = sync_xdb.put(a)
    a2 = sync_xdb.get(_id)
    xarray.testing.assert_identical(a, a2)


def test_nan_chunks(sync_xdb):
    """Test the case where the metadata of a dask array can't know the chunk sizes, as
    they are defined at the moment of computing it.

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
    _id, future = sync_xdb.put(a)
    future.compute()
    a2 = sync_xdb.get(_id)
    assert str(a2.shape) == "(nan,)"
    assert str(a2.chunks) == "((nan, nan),)"

    # second xarray bug
    # xarray.testing.assert_identical(a, a2)
    xarray.testing.assert_identical(
        xarray.DataArray(a.data.compute()), xarray.DataArray(a2.data.compute())
    )


def test_meta_not_found(sync_xdb):
    with pytest.raises(DocumentNotFoundError) as ex:
        sync_xdb.get(bson.ObjectId("deadbeefdeadbeefdeadbeef"))
    assert str(ex.value) == "deadbeefdeadbeefdeadbeef"


def test_no_segments_found(sync_xdb):
    _id, future = sync_xdb.put(ds)
    future.compute()
    sync_xdb.chunks.delete_many({"name": "d", "chunk": [1, 0]})
    ds2 = sync_xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}"
    )


# A missing chunk with chunk_size_bytes=2 causes np.frombuffer to crash with
# 'ValueError: buffer size must be a multiple of element size'.
# A missing chunk with chunk_size_bytes=8 causes ndarray.reshape to crash with
# 'ValueError: cannot reshape array of size 1 into shape (1,2)'.
@pytest.mark.parametrize("chunk_size_bytes", (2, 8))
def test_some_segments_not_found(sync_xdb, chunk_size_bytes):
    sync_xdb.chunk_size_bytes = chunk_size_bytes
    _id, future = sync_xdb.put(ds)
    future.compute()
    sync_xdb.chunks.delete_one({"name": "d", "chunk": [1, 0], "n": 1})
    ds2 = sync_xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}"
    )
