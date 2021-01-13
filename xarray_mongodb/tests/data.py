"""Shared test data between test_sync and test_async
"""
import dask.array as da
import numpy
import pytest
import xarray

ds = xarray.Dataset(
    coords={"x": (("x",), [1, 2]), "x2": (("x",), [3, 4]), "x3": (("x",), [5, 6])},
    data_vars={
        "d": (("x", "y"), [[10, 20], [30, 40]]),
        "s": da.from_array(1.0, chunks=()),
    },
    attrs={"foo": "bar"},
)
# In Linux/MacOSX, the default dtype for int is int64. In Windows, it's int32.
ds["d"] = ds["d"].astype("i8").chunk({"x": 1, "y": 2})
ds["x"] = ds["x"].astype("i8")
ds["x2"] = ds["x2"].astype("i8")
ds["x3"] = ds["x3"].astype("i8").chunk(1)
ds["d"].attrs["d_attr"] = 1
ds["x"].attrs["x_attr"] = 2


parametrize_roundtrip = pytest.mark.parametrize(
    "compute,load,embed_threshold_bytes,chunks,",
    [
        (
            False,
            None,
            0,
            {"x": None, "x2": None, "x3": ((1, 1),), "d": ((1, 1), (2,)), "s": ()},
        ),
        (
            False,
            None,
            2 ** 20,
            {"x": None, "x2": None, "x3": ((1, 1),), "d": ((1, 1), (2,)), "s": ()},
        ),
        (
            False,
            False,
            0,
            {"x": None, "x2": ((2,),), "x3": ((1, 1),), "d": ((1, 1), (2,)), "s": ()},
        ),
        (
            True,
            False,
            2 ** 20,
            {"x": None, "x2": None, "x3": None, "d": None, "s": None},
        ),
        (
            False,
            ["d"],
            0,
            {"x": None, "x2": ((2,),), "x3": ((1, 1),), "d": None, "s": ()},
        ),
        (True, None, 0, {"x": None, "x2": None, "x3": None, "d": None, "s": None}),
        (
            True,
            False,
            0,
            {"x": None, "x2": ((2,),), "x3": ((2,),), "d": ((2,), (2,)), "s": ()},
        ),
        (True, True, 0, {"x": None, "x2": None, "x3": None, "d": None, "s": None}),
        (True, ["d"], 0, {"x": None, "x2": ((2,),), "x3": ((2,),), "d": None, "s": ()}),
    ],
)


def expect_ds_meta(_id):
    return [
        {
            "_id": _id,
            "attrs": {"foo": "bar"},
            "chunkSize": 261120,
            "coords": {
                "x": {
                    "chunks": None,
                    "dims": ["x"],
                    "dtype": "<i8",
                    "shape": [2],
                    "type": "ndarray",
                    "attrs": {"x_attr": 2},
                },
                "x2": {
                    "chunks": None,
                    "dims": ["x"],
                    "dtype": "<i8",
                    "shape": [2],
                    "type": "ndarray",
                },
                "x3": {
                    "chunks": [[1, 1]],
                    "dims": ["x"],
                    "dtype": "<i8",
                    "shape": [2],
                    "type": "ndarray",
                },
            },
            "data_vars": {
                "d": {
                    "chunks": [[1, 1], [2]],
                    "dims": ["x", "y"],
                    "dtype": "<i8",
                    "shape": [2, 2],
                    "type": "ndarray",
                    "attrs": {"d_attr": 1},
                },
                "s": {
                    "chunks": [],
                    "dims": [],
                    "dtype": "<f8",
                    "shape": [],
                    "type": "ndarray",
                },
            },
        },
    ]


def expect_ds_chunks(_id):
    return [
        {
            "chunk": [0, 0],
            "data": b"\n\x00\x00\x00\x00\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "d",
            "shape": [1, 2],
            "type": "ndarray",
        },
        {
            "chunk": [1, 0],
            "data": b"\x1e\x00\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "d",
            "shape": [1, 2],
            "type": "ndarray",
        },
        {
            "chunk": [],
            "data": b"\x00\x00\x00\x00\x00\x00\xf0?",
            "dtype": "<f8",
            "meta_id": _id,
            "n": 0,
            "name": "s",
            "shape": [],
            "type": "ndarray",
        },
        {
            "chunk": None,
            "data": b"\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "x",
            "shape": [2],
            "type": "ndarray",
        },
        {
            "chunk": None,
            "data": b"\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "x2",
            "shape": [2],
            "type": "ndarray",
        },
        {
            "chunk": [0],
            "data": b"\x05\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "x3",
            "shape": [1],
            "type": "ndarray",
        },
        {
            "chunk": [1],
            "data": b"\x06\x00\x00\x00\x00\x00\x00\x00",
            "dtype": "<i8",
            "meta_id": _id,
            "n": 0,
            "name": "x3",
            "shape": [1],
            "type": "ndarray",
        },
    ]


da = xarray.DataArray(
    numpy.array([1], dtype="i1"), dims=["x"], coords={"x": ["x1"]}, attrs={"foo": 1}
)


def expect_da_meta(_id, name):
    out = [
        {
            "_id": _id,
            "attrs": {"foo": 1},
            "chunkSize": 261120,
            "coords": {
                "x": {
                    "chunks": None,
                    "dims": ["x"],
                    "dtype": "<U2",
                    "shape": [1],
                    "type": "ndarray",
                }
            },
            "data_vars": {
                "__DataArray__": {
                    "chunks": None,
                    "dims": ["x"],
                    "dtype": "|i1",
                    "shape": [1],
                    "type": "ndarray",
                }
            },
        }
    ]
    if name:
        out[0]["name"] = name
    return out


def expect_da_chunks(_id):
    return [
        {
            "chunk": None,
            "data": b"x\x00\x00\x001\x00\x00\x00",
            "dtype": "<U2",
            "meta_id": _id,
            "n": 0,
            "name": "x",
            "shape": [1],
            "type": "ndarray",
        },
        {
            "chunk": None,
            "data": b"\x01",
            "dtype": "|i1",
            "meta_id": _id,
            "n": 0,
            "name": "__DataArray__",
            "shape": [1],
            "type": "ndarray",
        },
    ]


def expect_meta_minimal(_id):
    """Metadata of an empty dataset"""
    return [
        {"_id": _id, "chunkSize": 261120, "coords": {}, "data_vars": {}},
    ]
