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
import uuid
import pytest
import xarray


LOAD_PICKLE = os.path.join(os.path.dirname(__file__), "load_pickle.py")


@pytest.fixture
def xdb():
    import pymongo
    from xarray_mongodb import XarrayMongoDB

    client = pymongo.MongoClient()
    dbname = "test_xarray_mongodb"
    coll = str(uuid.uuid4())
    yield XarrayMongoDB(client[dbname], coll)
    client.drop_database(dbname)


def test_pickle(xdb, tmpdir):
    ds = xarray.DataArray([1, 2]).chunk()
    _, future = xdb.put(ds)
    assert future is not None

    with open(f"{tmpdir}/p.pickle", "wb") as fh:
        pickle.dump(future, fh)
    subprocess.check_call([sys.executable, LOAD_PICKLE, f"{tmpdir}/p.pickle"])
