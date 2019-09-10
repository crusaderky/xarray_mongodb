"""Shared test data between test_sync and test_async
"""
import dask.array as da
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


parametrize_roundtrip = pytest.mark.parametrize(
    "compute,load,chunks",
    [
        (
            False,
            None,
            {"x": None, "x2": None, "x3": ((1, 1),), "d": ((1, 1), (2,)), "s": ()},
        ),
        (
            False,
            False,
            {"x": None, "x2": ((2,),), "x3": ((1, 1),), "d": ((1, 1), (2,)), "s": ()},
        ),
        (False, True, {"x": None, "x2": None, "x3": None, "d": None, "s": None}),
        (False, ["d"], {"x": None, "x2": ((2,),), "x3": ((1, 1),), "d": None, "s": ()}),
        (True, None, {"x": None, "x2": None, "x3": None, "d": None, "s": None}),
        (
            True,
            False,
            {"x": None, "x2": ((2,),), "x3": ((2,),), "d": ((2,), (2,)), "s": ()},
        ),
        (True, True, {"x": None, "x2": None, "x3": None, "d": None, "s": None}),
        (True, ["d"], {"x": None, "x2": ((2,),), "x3": ((2,),), "d": None, "s": ()}),
    ],
)


def expect_meta(_id):
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
                },
                "s": {
                    "chunks": [],
                    "dims": [],
                    "dtype": "<f8",
                    "shape": [],
                    "type": "ndarray",
                },
            },
        }
    ]


def expect_chunks(_id):
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
