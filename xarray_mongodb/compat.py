"""Import pint and sparse objects or dummies

Please read :doc:`nep18`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__all__ = ("COO", "Quantity", "Unit", "UnitRegistry", "has_motor")


try:
    import motor

    has_motor = motor.version_tuple >= (2, 3)
except ImportError:
    has_motor = False


# sparse imports or dummies
try:
    from sparse import COO

except ImportError:
    # Dummy objects that are not equal to anything else and for which isinstance()
    # always returns False. Can't just use `COO = object()` because things like
    # Optional[COO] don't like it.

    class COO:  # type: ignore[no-redef]
        pass


# pint imports or dummies
try:
    from pint import Quantity, Unit, UnitRegistry

except ImportError:

    class Unit:  # type: ignore[no-redef]
        def __init__(self, s: str):
            raise NotImplementedError("STUB")

    class Quantity:  # type: ignore[no-redef]
        magnitude: Any
        units: Unit

        def __init__(self, magnitude: Any, units: str | None = None):
            raise NotImplementedError("STUB")

    class UnitRegistry:  # type: ignore[no-redef]
        Quantity: Callable[..., Quantity]
        Unit: Callable[..., Unit]
