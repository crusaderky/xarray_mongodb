import os.path
import pickle
import subprocess
import sys
import xarray
from . import xdb  # noqa: F401


LOAD_PICKLE = os.path.join(os.path.dirname(__file__), 'load_pickle.py')


def test_pickle(xdb, tmpdir):  # noqa: F811
    ds = xarray.Dataset({'d': ('x', [1, 2])}).chunk()
    _, future = xdb.put(ds)
    assert future is not None

    with open(f'{tmpdir}/p.pickle', 'wb') as fh:
        pickle.dump(future, fh)
    subprocess.check_call([sys.executable, LOAD_PICKLE, f'{tmpdir}/p.pickle'])
