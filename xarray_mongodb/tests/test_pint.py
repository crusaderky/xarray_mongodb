import dask.array as da
import numpy as np
import pytest
import xarray

from xarray_mongodb import XarrayMongoDB

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
    prev = pint._APP_REGISTRY
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
def test_ureg(ureg, ureg_custom, sync_xdb):
    from xarray_mongodb import XarrayMongoDB

    assert sync_xdb.ureg is ureg
    sync_xdb_custom = XarrayMongoDB(sync_xdb.meta.database, ureg=ureg_custom)
    assert sync_xdb_custom.ureg is ureg_custom


@requires_motor
@requires_pint
def test_ureg_motor(ureg, ureg_custom):
    """The only impact on sync.py and asyncio.py is on the ureg property. Everything
    else is in common.py/chunks.py and can be just tested using the sync client.
    """
    import motor.motor_asyncio
    from xarray_mongodb import XarrayMongoDBAsyncIO

    db = motor.motor_asyncio.AsyncIOMotorClient().test_xarray_mongodb

    sync_xdb = XarrayMongoDBAsyncIO(db)
    assert sync_xdb.ureg is ureg

    sync_xdb = XarrayMongoDBAsyncIO(db, ureg=ureg_custom)
    assert sync_xdb.ureg is ureg_custom


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
            "attrs": {},
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
def bad_meta1(ureg, sync_xdb):
    """dask _meta is the wrong Quantity
    """
    a = xarray.DataArray(
        da.from_array(
            ureg.Quantity(123, "kg"),
            asarray=False,
            meta=ureg.Quantity(np.array([], dtype=int), "g"),
        )
    )
    assert str(a.data.compute().units) == "kg"
    assert str(a.data._meta.units) == "g"

    _id, future = sync_xdb.put(a)
    future.compute()
    b = sync_xdb.get(_id)

    assert str(b.data.compute().units) == "kg"
    assert str(b.data._meta.units) == "g"
    xarray.testing.assert_identical(a, b)


@requires_pint
def bad_meta2(ureg, sync_xdb):
    """dask _meta is a Quantity, but dask payload is a np.ndarray
    """
    a = xarray.DataArray(
        da.from_array(np.array(123), meta=ureg.Quantity(np.array([], dtype=int), "g"))
    )
    assert str(a.data._meta.units) == "g"

    _id, future = sync_xdb.put(a)
    future.compute()
    b = sync_xdb.get(_id)

    assert str(b.data._meta.units) == "g"
    xarray.testing.assert_identical(a, b)


@requires_pint
def bad_meta3(ureg, sync_xdb):
    """dask _meta is a np.ndarray, but dask payload is a Quantity
    """
    a = xarray.DataArray(
        da.from_array(
            ureg.Quantity(123, "kg"), asarray=False, meta=np.array([], dtype=int)
        )
    )
    assert str(a.data.compute().units) == "kg"

    _id, future = sync_xdb.put(a)
    future.compute()
    b = sync_xdb.get(_id)

    assert str(b.data.compute().units) == "kg"
    xarray.testing.assert_identical(a, b)


@requires_pint
def custom_units(ureg, ureg_custom, sync_xdb):
    """dask _meta is a np.ndarray, but dask payload is a Quantity
    """
    import pint

    a = xarray.DataArray(ureg_custom.Quantity(1, "test_unit"))
    assert str(a.data.units) == "test_unit"

    sync_xdb_custom = XarrayMongoDB(
        sync_xdb.meta.database,
        collection=sync_xdb.meta.name.split(".")[0],
        ureg=ureg_custom,
    )

    _id, future = sync_xdb_custom.put(a)
    future.compute()
    b = sync_xdb_custom.get(_id)

    assert str(b.data.compute().units) == "test_unit"
    xarray.testing.assert_identical(a, b)

    with pytest.raises(pint.UndefinedUnitError):
        sync_xdb.get(_id)

    pint.set_application_registry(ureg_custom)
    try:
        c = sync_xdb.get(_id)
        assert str(c.data.compute().units) == "test_unit"
        xarray.testing.assert_identical(a, c)
    finally:
        pint.set_application_registry(ureg)


@pytest.mark.xfail(reason="xarray->pint->dask broken upstream: pint#878")
@requires_pint
def test_dask(ureg, sync_xdb):
    a = sample_data(ureg).chunk(1)
    _id, future = sync_xdb.put(a)
    future.compute()
    b = sync_xdb.get(_id)
    xarray.testing.assert_identical(a, b)

    for k, v in a.variables.items():
        assert b[k].chunks == v.chunks
        assert b[k].data._meta.units == v.data._meta.units
        assert b[k].compute().data.units == v.compute().data.units
