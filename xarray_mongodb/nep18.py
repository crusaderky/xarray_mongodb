"""Import pint and sparse objects or dummies

Please read :doc:`nep18`.
"""
from typing import Any, Callable

__all__ = ("COO", "Quantity", "Unit", "UnitRegistry")


# sparse imports or dummies
try:
    from sparse import COO

except ImportError:
    # Dummy objects that are not equal to anything else and for which isinstance()
    # always returns False. Can't just use `COO = object()` because things like
    # Optional[COO] don't like it.

    class COO:  # type: ignore
        pass


# pint imports or dummies
try:
    from pint import UnitRegistry
    from pint.quantity import _Quantity as Quantity
    from pint.unit import _Unit as Unit

except ImportError:

    class Unit:  # type: ignore
        def __init__(self, s):
            raise NotImplementedError("STUB")

    class Quantity:  # type: ignore
        magnitude: Any
        units: Unit

        def __init__(self, magnitude, units=None):
            raise NotImplementedError("STUB")

    class UnitRegistry:  # type: ignore
        Quantity: Callable[..., Quantity]
        Unit: Callable[..., Unit]
