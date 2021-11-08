import importlib

import pytest
from packaging.version import parse


def _import_or_skip(*args):
    """Build skip markers for a optional module

    :param args:
        Tuples of (module name, min version)
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

            has_this = parse(version) >= parse(minversion)

        if not has_this:
            mod_str.append(f"{modname}>={minversion}")

        has = has and has_this

    reason = "requires " + ", ".join(mod_str)
    func = pytest.mark.skipif(not has, reason=reason)
    return has, func


has_motor, requires_motor = _import_or_skip(("motor", "2.0"))
has_pint, requires_pint = _import_or_skip(
    ("pint", "0.10"), ("numpy", "1.18"), ("dask", "2.24"), ("xarray", "0.13"),
)
has_sparse, requires_sparse = _import_or_skip(
    ("sparse", "0.8"), ("numpy", "1.18"), ("dask", "2.24"), ("xarray", "0.13"),
)


def assert_chunks_index(indices: list):
    """Test that the custom index on the xarray.chunks collection is well-formed"""
    keys = [dict(idx["key"]) for idx in indices]
    assert keys == [{"_id": 1}, {"meta_id": 1, "name": 1, "chunk": 1}]
