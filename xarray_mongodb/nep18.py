"""Tools for pint and sparse libraries.

Please read :doc:`nep18`.
"""
from typing import Any, Callable, Optional, Union

import dask.array as da
import numpy as np
import xarray

try:
    from pint.unit import _Unit as Unit  # type: ignore
    from pint.quantity import _Quantity as Quantity  # type: ignore
    from pint import UnitRegistry  # type: ignore

except ImportError:
    # Dummy objects that are not equal to anything else and for which isinstance()
    # always returns False. Can't just use `Quantity = object()` because the definition
    # of EagerArray below won't like it.
    class Unit:  # type: ignore
        pass

    class Quantity:  # type: ignore
        magnitude: Any
        units: Unit

    class UnitRegistry:  # type: ignore
        Quantity: Callable[..., Quantity]  # type: ignore
        Unit: Callable[..., Unit]  # type: ignore


try:
    from sparse import COO  # type: ignore

except ImportError:

    class COO:  # type: ignore
        pass


EagerArray = Union[np.ndarray, COO, Quantity]


def get_meta(x: da.Array) -> Any:
    """Wrapper around ``da.Array._meta`` to cope with dask < 2.0
    """
    try:
        return x._meta
    except AttributeError:
        # dask < 2.0
        return np.array([], dtype=x.dtype)


def get_units(x: xarray.Variable) -> Optional[str]:
    """Extract string representation of pint.Units out of a variable

    TODO Upstream discussion to have .units and .to_units() methods added to
         the xarray objects themselves
    """
    data = x.data

    if isinstance(data, da.Array):
        data = get_meta(data)
    if isinstance(data, Quantity):
        return str(data.units)
    if isinstance(data, (np.ndarray, COO)):
        return None
    raise TypeError(data)
