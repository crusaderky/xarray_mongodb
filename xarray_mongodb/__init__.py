import importlib.metadata
from typing import Any

from xarray_mongodb import patch_pymongo
from xarray_mongodb.compat import has_motor
from xarray_mongodb.errors import DocumentNotFoundError
from xarray_mongodb.sync import XarrayMongoDB

try:
    __version__ = importlib.metadata.version("xarray_mongodb")
except importlib.metadata.PackageNotFoundError:  # pragma: nocover
    # Local copy, not installed with pip
    __version__ = "9999"


# Make PyMongo objects serializable
patch_pymongo.patch_pymongo()
del patch_pymongo

if has_motor:
    from xarray_mongodb.asyncio import XarrayMongoDBAsyncIO
else:

    class XarrayMongoDBAsyncIO:  # type: ignore[no-redef]
        def __new__(cls, *args: Any, **kwargs: Any) -> Any:  # noqa: ARG003
            raise ImportError("XarrayMongoDBAsyncIO requires motor >=2.3")


del has_motor

# Correct inheritance and type annotations in intersphinx
DocumentNotFoundError.__module__ = "xarray_mongodb"
XarrayMongoDB.__module__ = "xarray_mongodb"
XarrayMongoDBAsyncIO.__module__ = "xarray_mongodb"

__all__ = (
    "DocumentNotFoundError",
    "XarrayMongoDB",
    "XarrayMongoDBAsyncIO",
    "__version__",
)
