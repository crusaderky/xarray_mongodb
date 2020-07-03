"""pytest fixtures
"""
import asyncio

import pymongo
import pytest

from xarray_mongodb import XarrayMongoDB, XarrayMongoDBAsyncIO


@pytest.fixture(scope="function")
def sync_db():
    client = pymongo.MongoClient()
    dbname = "test_xarray_mongodb"
    yield client[dbname]
    client.drop_database(dbname)


@pytest.fixture(scope="function")
def sync_xdb(sync_db):
    return XarrayMongoDB(sync_db, embed_threshold_bytes=0)


@pytest.fixture(scope="function")
def event_loop():
    """pytest-asyncio by default creates a new event loop or every new coroutine. This
    happens *after* the fixtures are applied, which causes the AsyncIOMotorClient to be
    attached to the wrong event loop
    """
    yield asyncio.get_event_loop()


@pytest.fixture(scope="function")
async def async_db(event_loop):
    import motor.motor_asyncio

    client = motor.motor_asyncio.AsyncIOMotorClient()
    dbname = "test_xarray_mongodb"
    yield client[dbname]
    await client.drop_database(dbname)


@pytest.fixture(scope="function")
async def async_xdb(async_db):
    return XarrayMongoDBAsyncIO(async_db, embed_threshold_bytes=0)
