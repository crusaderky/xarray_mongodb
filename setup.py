#!/usr/bin/env python
import os
import re
import warnings

from setuptools import find_packages, setup

MAJOR = 0
MINOR = 1
MICRO = 0
ISRELEASED = True
VERSION = "%d.%d.%d" % (MAJOR, MINOR, MICRO)
QUALIFIER = ""


DISTNAME = "xarray_mongodb"
LICENSE = "Apache"
AUTHOR = "Amphora, Inc."
AUTHOR_EMAIL = "support@amphorainc.com"
URL = "https://github.com/AmphoraInc/xarray_mongodb"
CLASSIFIERS = [
    # https://pypi.org/pypi?%3Aaction=list_classifiers
    "Development Status :: 3 - Alpha",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Intended Audience :: Science/Research",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Topic :: Scientific/Engineering",
]

INSTALL_REQUIRES = [
    "numpy >= 1.13",
    "pandas >= 0.21",
    "dask >= 1.1",
    "xarray >= 0.10.4",
    "pymongo >= 3.7",
]
EXTRAS_REQUIRE = {
    "asyncio": ["motor >= 2.0"],
    "pint": ["pint >= 0.9", "numpy >= 1.16", "xarray > 0.12.3"],
    "sparse": ["sparse >= 0.8", "numpy >= 1.16", "xarray > 0.12.3"],
}
TESTS_REQUIRE = ["pytest >= 3.7", "pytest-asyncio >= 0.10"]

DESCRIPTION = "xarray_mongodb"
LONG_DESCRIPTION = """

"""  # noqa

# Code to extract and write the version copied from pandas.
# Used under the terms of pandas's license.
FULLVERSION = VERSION
write_version = True

if not ISRELEASED:
    import subprocess

    FULLVERSION += ".dev"

    pipe = None
    for cmd in ["git", "git.cmd"]:
        try:
            pipe = subprocess.Popen(
                [cmd, "describe", "--always", "--match", "v[0-9]*"],
                stdout=subprocess.PIPE,
            )
            (so, serr) = pipe.communicate()
            if pipe.returncode == 0:
                break
        except BaseException:
            pass

    if pipe is None or pipe.returncode != 0:
        # no git, or not in git dir
        if os.path.exists("xarray_mongodb/version.py"):
            warnings.warn(
                "WARNING: Couldn't get git revision,"
                " using existing xarray_mongodb/version.py"
            )
            write_version = False
        else:
            warnings.warn(
                "WARNING: Couldn't get git revision; using generic version string"
            )
    else:
        # have git, in git dir, but may have used a shallow clone (travis does
        # this)
        rev = so.strip()
        rev = rev.decode("ascii")

        if not rev.startswith("v") and re.match("[a-zA-Z0-9]{7,9}", rev):
            # partial clone, manually construct version string this is the format before
            # we started using git-describe to get an ordering on dev version strings.
            rev = "v%s+dev.%s" % (VERSION, rev)

        # Strip leading v from tags format "vx.y.z" to get th version string
        FULLVERSION = rev.lstrip("v")

        # make sure we respect PEP 440
        FULLVERSION = FULLVERSION.replace("-", "+dev", 1).replace("-", ".")

else:
    FULLVERSION += QUALIFIER


def write_version_py():
    contents = 'version = "%s"\nshort_version = "%s"\n'
    filename = os.path.join(os.path.dirname(__file__), "xarray_mongodb", "version.py")
    with open(filename, "w") as fh:
        fh.write(contents % (FULLVERSION, VERSION))


if write_version:
    write_version_py()

setup(
    name=DISTNAME,
    version=FULLVERSION,
    license=LICENSE,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    classifiers=CLASSIFIERS,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    tests_require=TESTS_REQUIRE,
    python_requires=">=3.6",
    url=URL,
    packages=find_packages(),
    package_data={"xarray_mongodb": ["py.typed"]},
)
