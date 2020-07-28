import pkg_resources

from . import patch_pymongo
from .errors import DocumentNotFoundError  # noqa: F401
from .sync import XarrayMongoDB  # noqa: F401

try:
    __version__ = pkg_resources.get_distribution("xarray_mongodb").version
except Exception:  # pragma: no cover
    # No installed copy
    __version__ = "999"

# Make PyMongo objects serializable
patch_pymongo.patch_pymongo()
del patch_pymongo


try:
    import motor

    has_motor = motor.version_tuple >= (2, 0)
except ImportError:
    has_motor = False

if has_motor:
    from .asyncio import XarrayMongoDBAsyncIO  # noqa: F401
else:

    class XarrayMongoDBAsyncIO:  # type: ignore
        def __new__(cls, *args, **kwargs):
            raise ImportError("XarrayMongoDBAsyncIO requires motor >=2.0")


del has_motor

# Correct inheritance and type annotations in intersphinx
DocumentNotFoundError.__module__ = "xarray_mongodb"
XarrayMongoDB.__module__ = "xarray_mongodb"
XarrayMongoDBAsyncIO.__module__ = "xarray_mongodb"

__all__ = (
    "__version__",
    "DocumentNotFoundError",
    "XarrayMongoDB",
    "XarrayMongoDBAsyncIO",
)
