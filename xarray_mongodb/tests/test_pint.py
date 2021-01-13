import dask.array as da
import numpy as np
import pytest
import xarray

from xarray_mongodb import XarrayMongoDB, XarrayMongoDBAsyncIO

from . import requires_motor, requires_pint


@pytest.fixture
def ureg():
    import pint

    return pint._APP_REGISTRY


@pytest.fixture
def ureg_custom():
    import pint

    ureg = pint.UnitRegistry()
    ureg.define("test_unit = 123 kg")
    return ureg


@pytest.fixture
def ureg_custom_global():
    import pint

    ureg = pint.UnitRegistry()
    ureg.define("test_unit = 123 kg")
    prev = pint.get_application_registry()
    pint.set_application_registry(ureg)
    yield ureg
    pint.set_application_registry(prev)


def sample_data(ureg):
    return xarray.Dataset(
        coords={
            # As of xarray 0.13, can't assign units to IndexVariables
            "x": ((), ureg.Quantity(np.array(123, dtype="i8"), "kg")),
            "y": (("dim_0",), ureg.Quantity(np.array([1, 2], dtype="i8"), "m")),
        },
        data_vars={"d": (("dim_0",), ureg.Quantity(np.array([3, 4], dtype="i8"), "s"))},
    )


@requires_pint
def test_ureg(ureg, ureg_custom, sync_db, sync_xdb):
    assert sync_xdb.ureg is ureg
    xdb2 = XarrayMongoDB(sync_db, ureg=ureg_custom)
    assert xdb2.ureg is ureg_custom
    xdb2.ureg = ureg
    assert xdb2.ureg is ureg


@requires_motor
@requires_pint
def test_ureg_motor(ureg, ureg_custom, async_db, async_xdb):
    """The only impact on sync.py and asyncio.py is on the ureg property. Everything
    else is in common.py/chunks.py and can be just tested using the sync client.
    """
    assert async_xdb.ureg is ureg
    xdb2 = XarrayMongoDBAsyncIO(async_db, ureg=ureg_custom)
    assert xdb2.ureg is ureg_custom
    xdb2.ureg = ureg
    assert xdb2.ureg is ureg


@requires_pint
def test_ureg_global(ureg, ureg_custom_global, sync_xdb):
    assert sync_xdb.ureg is ureg_custom_global


@requires_motor
@requires_pint
def test_ureg_motor_global(ureg, ureg_custom_global, async_xdb):
    assert async_xdb.ureg is ureg_custom_global


@requires_pint
def test_numpy(ureg, sync_xdb):
    a = sample_data(ureg)
    _id, _ = sync_xdb.put(a)
    b = sync_xdb.get(_id)
    xarray.testing.assert_identical(a, b)


@requires_pint
def test_db_contents(ureg, sync_xdb):
    ds = sample_data(ureg)
    _id, _ = sync_xdb.put(ds)

    assert list(sync_xdb.meta.find()) == [
        {
            "_id": _id,
            "chunkSize": 261120,
            "coords": {
                "x": {
                    "chunks": None,
                    "dims": [],
                    "dtype": "<i8",
                    "shape": [],
                    "type": "ndarray",
                    "units": "kilogram",
                },
                "y": {
                    "chunks": None,
                    "dims": ["dim_0"],
                    "dtype": "<i8",
                    "shape": [2],
                    "type": "ndarray",
                    "units": "meter",
                },
            },
            "data_vars": {
                "d": {
                    "chunks": None,
                    "dims": ["dim_0"],
                    "dtype": "<i8",
                    "shape": [2],
                    "type": "ndarray",
                    "units": "second",
                }
            },
        }
    ]

    chunks = list(sync_xdb.chunks.find())
    for chunk in chunks:
        del chunk["_id"]
    assert chunks == [
        {
            "chunk": None,
            "data": b"\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "d",
            "shape": [2],
            "type": "ndarray",
        },
        {
            "chunk": None,
            "data": b"{\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "x",
            "shape": [],
            "type": "ndarray",
        },
        {
            "chunk": None,
            "data": b"\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "y",
            "shape": [2],
            "type": "ndarray",
        },
    ]


@requires_pint
def test_custom_units(ureg, ureg_custom, sync_xdb):
    """dask _meta is a np.ndarray, but dask payload is a Quantity"""
    import pint

    a = xarray.DataArray(ureg_custom.Quantity([1], "test_unit"))
    assert str(a.data.units) == "test_unit"

    sync_xdb_custom = XarrayMongoDB(
        sync_xdb.meta.database,
        collection=sync_xdb.meta.name.split(".")[0],
        ureg=ureg_custom,
        embed_threshold_bytes=0,
    )

    _id, _ = sync_xdb_custom.put(a)
    b = sync_xdb_custom.get(_id)

    assert str(b.data.units) == "test_unit"
    xarray.testing.assert_identical(a, b)

    with pytest.raises(pint.UndefinedUnitError):
        sync_xdb.get(_id)

    pint.set_application_registry(ureg_custom)
    try:
        c = sync_xdb.get(_id)
        assert str(c.data.units) == "test_unit"
        xarray.testing.assert_identical(a, c)
    finally:
        pint.set_application_registry(ureg)


@requires_pint
def test_scalar_dtypes(ureg, sync_xdb):
    """DataArray(numpy.float64(1.2)) automatically converts the data to a scalar
    numpy.ndarray. DataArray(Quantity(numpy.float64(1.2))) doesn't, but it's ok because
    numpy dtypes are array-likes.
    """
    a = xarray.DataArray(ureg.Quantity(np.float32(1.2), "s"))
    _id, _ = sync_xdb.put(a)
    b = sync_xdb.get(_id)
    np.testing.assert_array_equal(b.data.magnitude, np.array(1.2, dtype="f4"))

    assert list(sync_xdb.meta.find()) == [
        {
            "_id": _id,
            "chunkSize": 261120,
            "coords": {},
            "data_vars": {
                "__DataArray__": {
                    "chunks": None,
                    "dims": [],
                    "dtype": "<f4",
                    "shape": [],
                    "type": "ndarray",
                    "units": "second",
                }
            },
        }
    ]

    chunks = list(sync_xdb.chunks.find())
    for chunk in chunks:
        del chunk["_id"]
    assert chunks == [
        {
            "chunk": None,
            "data": b"\x9a\x99\x99?",
            "dtype": "<f4",
            "meta_id": _id,
            "n": 0,
            "name": "__DataArray__",
            "shape": [],
            "type": "ndarray",
        }
    ]


@requires_pint
def test_dask(ureg, sync_xdb):
    a = xarray.DataArray(ureg.Quantity(da.arange(4, dtype="i1", chunks=2), "kg"))

    _id, future = sync_xdb.put(a)
    future.compute()
    b = sync_xdb.get(_id)

    assert str(b.data.units) == "kilogram"
    np.testing.assert_array_equal(b.data.magnitude._meta, a.data.magnitude._meta)
    assert b.data.magnitude.chunks == a.data.magnitude.chunks
    np.testing.assert_array_equal(
        b.data.magnitude.compute(), a.data.magnitude.compute()
    )

    c = sync_xdb.get(_id, load=True)
    assert str(c.data.units) == "kilogram"
    assert isinstance(c.data.magnitude, np.ndarray)
    np.testing.assert_array_equal(c.data.magnitude, a.data.magnitude.compute())

    assert list(sync_xdb.meta.find()) == [
        {
            "_id": _id,
            "chunkSize": 261120,
            "coords": {},
            "data_vars": {
                "__DataArray__": {
                    "chunks": [[2, 2]],
                    "dims": ["dim_0"],
                    "dtype": "|i1",
                    "shape": [4],
                    "type": "ndarray",
                    "units": "kilogram",
                }
            },
            "name": a.name,
        }
    ]

    chunks = sorted(sync_xdb.chunks.find(), key=lambda doc: doc["chunk"])
    for chunk in chunks:
        del chunk["_id"]

    assert chunks == [
        {
            "chunk": [0],
            "data": b"\x00\x01",
            "dtype": "|i1",
            "meta_id": _id,
            "n": 0,
            "name": "__DataArray__",
            "shape": [2],
            "type": "ndarray",
        },
        {
            "chunk": [1],
            "data": b"\x02\x03",
            "dtype": "|i1",
            "meta_id": _id,
            "n": 0,
            "name": "__DataArray__",
            "shape": [2],
            "type": "ndarray",
        },
    ]
