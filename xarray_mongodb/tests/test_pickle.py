"""Test that the pymongo.MongoClient embedded in the dask future can be
pickled/unpickled and sent over the network to dask distributed.

This requires a monkey-patch to pymongo (patch_pymongo.py) which must be loaded by the
interpreter BEFORE unpickling the pymongo.MongoClient object. This is achieved by always
having a function defined by xarray_mongodb before the pymongo.MongoClient object in the
tuples that constitute the dask graph.
"""
import multiprocessing

import xarray


def test_pickle(sync_xdb):
    da = xarray.DataArray([1, 2]).chunk()
    _, future = sync_xdb.put(da)
    assert not list(sync_xdb.chunks.find({}))
    assert future is not None

    # Run in an interpreter where xarray_mongodb hasn't been imported yet
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(target=future.compute)
    proc.start()
    proc.join()
    assert len(list(sync_xdb.chunks.find({}))) == 1
