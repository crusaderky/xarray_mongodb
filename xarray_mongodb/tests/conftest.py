"""pytest fixtures
"""
import asyncio
import uuid

import pytest


@pytest.fixture
def sync_xdb():
    import pymongo
    from xarray_mongodb import XarrayMongoDB

    client = pymongo.MongoClient()
    dbname = "test_xarray_mongodb"
    coll = str(uuid.uuid4())
    yield XarrayMongoDB(client[dbname], coll)
    client.drop_database(dbname)


@pytest.fixture
def event_loop():
    """pytest-asyncio by default creates a new event loop or every new coroutine. This
    happens *after* the fixtures are applied, which causes the AsyncIOMotorClient to be
    attached to the wrong event loop
    """
    yield asyncio.get_event_loop()


@pytest.fixture
async def async_xdb(event_loop):
    import motor.motor_asyncio
    from xarray_mongodb import XarrayMongoDBAsyncIO

    client = motor.motor_asyncio.AsyncIOMotorClient()
    dbname = "test_xarray_mongodb"
    coll = str(uuid.uuid4())
    yield XarrayMongoDBAsyncIO(client[dbname], coll)
    await client.drop_database(dbname)
