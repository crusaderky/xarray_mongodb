import importlib

import pytest


def _import_or_skip(*args, nep18: bool = False):
    """Build skip markers for a optional module

    :param args:
        Tuples of (module name, min version)
    :param bool nep18:
        Set to True if the functionality requires NEP18 to be active
    :return:
        Tuple of

        has_module (bool)
            True if the module is available and >= minversion
        requires_module (decorator)
            Tests decorated with it will only run if the module is available
            and >= minversion
    """
    has = True

    mod_str = []

    for modname, minversion in args:
        try:
            mod = importlib.import_module(modname)
        except ImportError:
            has = False
        else:
            try:
                version = mod.__version__  # type: ignore
            except AttributeError:
                version = mod.version  # type: ignore
            has = has and version >= minversion

    if not has:
        mod_str.append(f"{modname}>={minversion}")

    reason = "requires " + ", ".join(mod_str)
    if has and nep18:
        from xarray.core.npcompat import IS_NEP18_ACTIVE

        if not IS_NEP18_ACTIVE:
            has = False
            reason = "requires NEP18 to be activated"

    func = pytest.mark.skipif(not has, reason=reason)
    return has, func


has_motor, requires_motor = _import_or_skip(("motor", "2.0"))
has_pint, requires_pint = _import_or_skip(
    ("pint", "0.9"), ("numpy", "0.16"), ("xarray", "0.12.3+85"), nep18=True
)
has_sparse, requires_sparse = _import_or_skip(
    ("sparse", "0.8"), ("numpy", "0.16"), ("xarray", "0.12.3+85"), nep18=True
)
