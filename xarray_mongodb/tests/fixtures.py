"""pytest fixtures

.. note::
   These functions can't be decorated by ``@pytest.fixture`` in this module because it
   confuses flake8.

Usage::

    import pytest
    from .fixtures import sync_xdb
    xdb = pytest.fixture(sync_xdb)
"""
import uuid


def sync_xdb():
    import pymongo
    from xarray_mongodb import XarrayMongoDB

    client = pymongo.MongoClient()
    dbname = "test_xarray_mongodb"
    coll = str(uuid.uuid4())
    yield XarrayMongoDB(client[dbname], coll)
    client.drop_database(dbname)


async def async_xdb():
    import motor.motor_asyncio
    from xarray_mongodb import XarrayMongoDBAsyncIO

    client = motor.motor_asyncio.AsyncIOMotorClient()
    dbname = "test_xarray_mongodb"
    coll = str(uuid.uuid4())
    yield XarrayMongoDBAsyncIO(client[dbname], coll)
    await client.drop_database(dbname)
