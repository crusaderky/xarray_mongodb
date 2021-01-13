import importlib
import re

import pytest


def _parse_version(v_str):
    """Return sortable version of a dependency module"""
    # Some development versions are name 1.2.3+4
    # Others are 1.2.3.dev4. Normalize them.
    v_str = re.sub(r"\+.*", ".dev0", v_str)

    v_list = list(v_str.split("."))
    for i, v in enumerate(v_list):
        try:
            v_list[i] = int(v)
        except ValueError:  # pragma: nocover
            pass

    return tuple(v_list)


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
            has_this = False
        else:
            try:
                version = mod.__version__  # type: ignore
            except AttributeError:
                version = mod.version  # type: ignore

            has_this = _parse_version(version) >= _parse_version(minversion)

        if not has_this:
            mod_str.append(f"{modname}>={minversion}")

        has = has and has_this

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
    ("pint", "0.10"),
    ("numpy", "1.17"),
    ("dask", "2.0"),
    ("xarray", "0.13"),
    nep18=True,
)
has_sparse, requires_sparse = _import_or_skip(
    ("sparse", "0.8"),
    ("numpy", "1.17"),
    ("dask", "2.0"),
    ("xarray", "0.13"),
    nep18=True,
)


def assert_chunks_index(indices: list):
    """Test that the custom index on the xarray.chunks collection is well-formed"""
    keys = [dict(idx["key"]) for idx in indices]
    assert keys == [{"_id": 1}, {"meta_id": 1, "name": 1, "chunk": 1}]
