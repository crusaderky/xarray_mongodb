"""Test that the pymongo.MongoClient embedded in the dask future can be
pickled/unpickled and sent over the network to dask distributed.

This requires a monkey-patch to pymongo (patch_pymongo.py) which must be loaded by the
interpreter BEFORE unpickling the pymongo.MongoClient object. This is achieved by always
having a function defined by xarray_mongodb before the pymongo.MongoClient object in the
tuples that constitute the dask graph.
"""
import os.path
import pickle
import subprocess
import sys

import pytest
import xarray

from .fixtures import sync_xdb

xdb = pytest.fixture(sync_xdb)

LOAD_PICKLE = os.path.join(os.path.dirname(__file__), "load_pickle.py")


def test_pickle(xdb, tmpdir):
    ds = xarray.DataArray([1, 2]).chunk()
    _, future = xdb.put(ds)
    assert future is not None

    with open(f"{tmpdir}/p.pickle", "wb") as fh:
        pickle.dump(future, fh)
    subprocess.check_call([sys.executable, LOAD_PICKLE, f"{tmpdir}/p.pickle"])
