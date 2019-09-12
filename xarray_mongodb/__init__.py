try:
    from .version import version as __version__  # noqa: F401
except ImportError:  # pragma: no cover
    raise ImportError(
        "xarray_mongodb not properly installed. If you are running from the source "
        "directory, please instead create a new virtual environment (using conda or "
        "virtualenv) and then install it in-place by running: pip install -e ."
    )


from . import patch_pymongo
from .errors import DocumentNotFoundError  # noqa: F401
from .sync import XarrayMongoDB  # noqa: F401

# Make PyMongo objects serialisable
patch_pymongo.patch_pymongo()
del patch_pymongo

__all__ = ("__version__", "DocumentNotFoundError", "XarrayMongoDB")


try:
    import motor

    has_motor = motor.version_tuple >= (2, 0)
except ImportError:
    has_motor = False

if has_motor:
    from .asyncio import XarrayMongoDBAsyncIO  # noqa: F401

    __all__ = __all__ + ("XarrayMongoDBAsyncIO",)  # type: ignore

del has_motor
