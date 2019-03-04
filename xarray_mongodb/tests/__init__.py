import importlib
import uuid
import pytest


def _import_or_skip(modname, minversion=None):
    """Build skip markers for a optional module

    :param str modname:
        Name of the optional module
    :param str minversion:
        Minimum required version
    :return:
        Tuple of

        has_module (bool)
            True if the module is available and >= minversion
        requires_module (decorator)
            Tests decorated with it will only run if the module is available
            and >= minversion
    """
    reason = 'requires %s' % modname
    if minversion:
        reason += '>=%s' % minversion

    try:
        mod = importlib.import_module(modname)
        has = True
    except ImportError:
        has = False
    if has and minversion and mod.version < minversion:
        has = False

    func = pytest.mark.skipif(not has, reason=reason)
    return has, func


has_motor, requires_motor = _import_or_skip('motor', '2.0')


@pytest.fixture
def xdb():
    import pymongo
    from xarray_mongodb import XarrayMongoDB

    client = pymongo.MongoClient()
    dbname = 'test_xarray_mongodb-%s' % uuid.uuid4()
    yield XarrayMongoDB(client[dbname])
    client.drop_database(dbname)


@pytest.fixture
def xdb_async():
    import motor.motor_asyncio
    from xarray_mongodb import XarrayMongoDBAsyncIO

    client = motor.motor_asyncio.AsyncIOMotorClient()
    dbname = 'test_xarray_mongodb-%s' % uuid.uuid4()
    yield XarrayMongoDBAsyncIO(client[dbname])
    client.drop_database(dbname)
