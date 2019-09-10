"""Patch pymongo objects to make them serializable
"""
import pymongo
import pymongo.collection
import pymongo.database


def patch_pymongo() -> None:
    pymongo.MongoClient.__init__ = client_init
    pymongo.MongoClient.__getstate__ = client_getstate
    pymongo.MongoClient.__setstate__ = client_setstate
    pymongo.database.Database.__getstate__ = database_getstate
    pymongo.database.Database.__setstate__ = database_setstate
    pymongo.collection.Collection.__getstate__ = collection_getstate
    pymongo.collection.Collection.__setstate__ = collection_setstate


original_client_init = pymongo.MongoClient.__init__


def client_init(self, *args, **kwargs):
    original_client_init(self, *args, **kwargs)
    self.__args = args
    self.__kwargs = kwargs


def client_getstate(self):
    return self.__args, self.__kwargs


def client_setstate(self, state):
    args, kwargs = state
    self.__dict__ = pymongo.MongoClient(*args, **kwargs).__dict__


def database_getstate(self):
    return self.client, self.name


def database_setstate(self, state):
    client, name = state
    self.__dict__ = client[name].__dict__


def collection_getstate(self):
    return self.database, self.name


def collection_setstate(self, state):
    database, name = state
    self.__dict__ = database[name].__dict__
