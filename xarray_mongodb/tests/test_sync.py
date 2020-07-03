import bson
import pytest
import xarray

from xarray_mongodb import DocumentNotFoundError, XarrayMongoDB

from . import assert_chunks_index
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


def test_init(sync_db):
    xdb = XarrayMongoDB(sync_db, "foo", chunk_size_bytes=123, embed_threshold_bytes=456)
    assert xdb.meta.database is sync_db
    assert xdb.meta.name == "foo.meta"
    assert xdb.chunks.database is sync_db
    assert xdb.chunks.name == "foo.chunks"
    assert xdb.chunk_size_bytes == 123
    assert xdb.embed_threshold_bytes == 456


def test_index_on_put(sync_xdb):
    indices = list(sync_xdb.chunks.list_indexes())
    assert not indices
    sync_xdb.put(xarray.DataArray([1, 2]))
    indices = list(sync_xdb.chunks.list_indexes())
    assert_chunks_index(indices)


def test_index_on_get(sync_xdb):
    indices = list(sync_xdb.chunks.list_indexes())
    assert not indices
    _id = sync_xdb.meta.insert_one(
        {"coords": {}, "data_vars": {}, "chunkSize": 261120}
    ).inserted_id
    sync_xdb.get(_id)
    indices = list(sync_xdb.chunks.list_indexes())
    assert_chunks_index(indices)


@parametrize_roundtrip
@pytest.mark.parametrize("chunk_size_bytes", [16, 2 ** 20])
def test_roundtrip(
    sync_xdb, compute, load, chunks, embed_threshold_bytes, chunk_size_bytes
):
    sync_xdb.chunk_size_bytes = chunk_size_bytes
    sync_xdb.embed_threshold_bytes = embed_threshold_bytes

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


@pytest.mark.parametrize(
    "embed_threshold_bytes,expect_chunks",
    [
        (0, {"b", "c", "d", "e", "f"}),
        (8, {"b", "c", "d", "e"}),
        (24, {"b", "d", "e"}),
        (48, {"d", "e"}),
    ],
)
def test_embed_threshold(sync_xdb, embed_threshold_bytes, expect_chunks):
    ds = xarray.Dataset(
        data_vars={
            "a": ("x", []),
            "b": ("y", [1.1, 2.2, 3.3]),
            "c": ("z", [4.4, 5.5]),
            "d": ("x", []),  # dask variable
            "e": ("z", [1, 2]),  # dask variable
            "f": 1.23,
        }
    )
    ds["d"] = ds["d"].chunk()
    ds["e"] = ds["e"].chunk()

    assert sync_xdb.embed_threshold_bytes == 0
    sync_xdb.embed_threshold_bytes = embed_threshold_bytes
    _id, future = sync_xdb.put(ds)
    future.compute()

    ds2 = sync_xdb.get(_id)
    xarray.testing.assert_identical(ds, ds2)
    got_chunks = {chunk["name"] for chunk in sync_xdb.chunks.find({})}
    assert got_chunks == expect_chunks


def test_embed_everything(sync_xdb):
    a = xarray.DataArray([1, 2])
    assert sync_xdb.embed_threshold_bytes == 0
    sync_xdb.embed_threshold_bytes = 16

    _id, _ = sync_xdb.put(a)
    a2 = sync_xdb.get(_id)
    xarray.testing.assert_identical(a, a2)
    assert not list(sync_xdb.chunks.find({}))
