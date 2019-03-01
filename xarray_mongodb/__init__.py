try:
    from .version import version as __version__  # noqa: F401
except ImportError:  # pragma: no cover
    raise ImportError('xarray_mongodb not properly installed. If you are '
                      'running from the source directory, please instead '
                      'create a new virtual environment (using conda or '
                      'virtualenv) and then install it in-place by running: '
                      'pip install -e .')


# Make PyMongo objects serialisable
from . import patch_pymongo
patch_pymongo.patch_pymongo()


from .errors import DocumentNotFoundError  # noqa: F401
from .sync import XarrayMongoDB  # noqa: F401

try:
    import motor
    if motor.version_tuple[0] < 2:
        raise ImportError()
except ImportError:
    # Define stub so that the user gets a nice exception message when he tries
    # using the class
    class XarrayMongoDBAsyncIO:
        def __init__(self, *args, **kwargs):
            raise ImportError('Requires motor >= 2.0')
else:
    from .asyncio import XarrayMongoDBAsyncIO  # noqa: F401
