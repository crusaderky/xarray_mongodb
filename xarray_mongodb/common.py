"""Shared code between :class:`XarrayMongoDB` and :class:`XarrayMongoDBAsyncIO`
"""
from collections import OrderedDict, defaultdict
from functools import partial
from itertools import groupby
from typing import (
    Collection,
    DefaultDict,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import bson
import dask.array
import dask.base
import dask.core
import dask.highlevelgraph
from dask.delayed import Delayed
import pymongo
import xarray

from . import chunk
from .nep18 import EagerArray, UnitRegistry, get_units

CHUNKS_INDEX = [("meta_id", 1), ("name", 1), ("chunk", 1)]
CHUNK_SIZE_BYTES_DEFAULT = 255 * 1024


@dask.base.normalize_token.register(pymongo.collection.Collection)
def _(obj: pymongo.collection.Collection):
    """Single-dispatch function.
    Make pymongo collections tokenizable with dask.
    """
    client = obj.database.client
    # See patch_pymongo.py
    return client.__args, client.__kwargs, obj.database.name, obj.name


class XarrayMongoDBCommon:
    """Common functionality of :class:`XarrayMongoDB` and
    :class:`XarrayMongoDBAsyncIO`

    :param database:
        :class:`pymongo.database.Database` or
        :class:`motor.motor_asyncio.AsyncIOMotorDatabase`
    :param str collection:
        prefix of the collections to store the xarray data. Two collections will
        actually be created, <collection>.meta and <collection>.chunks.
    :param int chunk_size_bytes:
        Size of the payload in a document in the chunks collection. Not to be confused
        with dask chunks. dask chunks that are larger than chunk_size_bytes will be
        transparently split across multiple MongoDB documents.
    :param pint.unitregistry.UnitRegistry ureg:
        pint registry to allow putting and getting arrays with units.
    """

    chunk_size_bytes: int
    _ureg: Optional[UnitRegistry]
    _has_index: bool

    def __init__(
        self,
        database,
        collection: str,
        chunk_size_bytes: int,
        ureg: Optional[UnitRegistry],
    ):
        self.meta = database[collection].meta
        self.chunks = database[collection].chunks
        self.chunk_size_bytes = chunk_size_bytes
        self._ureg = ureg
        self._has_index = False

    @property
    def ureg(self) -> UnitRegistry:
        """Return:

        1. the registry that was defined in ``__init__``
        2. if it was omitted, the global registry defined with
           :func:`pint.set_application_registry`
        3. if the global registry was never set, a standard registry built with
           :file:`defaults_en.txt`
        """
        if self._ureg is not None:
            return self._ureg

        import pint

        return pint._APP_REGISTRY

    @ureg.setter
    def ureg(self, value: Optional[UnitRegistry]) -> None:
        self._ureg = value

    def _dataset_to_meta(self, x: Union[xarray.DataArray, xarray.Dataset]) -> dict:
        """Helper function of put().
        Convert a DataArray or Dataset into the dict to insert into the 'meta'
        collection.
        """
        meta = {
            "attrs": bson.SON(x.attrs),
            "coords": bson.SON(),
            "data_vars": bson.SON(),
            "chunkSize": self.chunk_size_bytes,
        }
        if isinstance(x, xarray.DataArray):
            if x.name:
                meta["name"] = x.name
            x = x.to_dataset(name="__DataArray__")

        for k, v in x.variables.items():
            subdoc = meta["coords"] if k in x.coords else meta["data_vars"]
            subdoc[k] = {
                "dims": v.dims,
                "dtype": v.dtype.str,
                "shape": v.shape,
                "chunks": v.chunks,
                "type": "ndarray",
            }
            units = get_units(v)
            if units:
                subdoc[k]["units"] = units

        return meta

    def _dataset_to_chunks(
        self, x: Union[xarray.DataArray, xarray.Dataset], meta_id: bson.ObjectId
    ) -> Tuple[List[dict], Optional[Delayed]]:
        """Helper method of put().
        Convert a DataArray or Dataset into the list of dicts to insert into the
        'chunks' collection. For dask variables, generate a
        :class:`~dask.delayed.Delayed` object.

        :param x:
            :class:`xarray.DataArray` or :class:`xarray.Dataset`
        :param bson.ObjectId meta_id:
            id_ of the document in the 'meta' collection
        :returns:
            tuple of

            chunks
                list of dicts to be inserted into the 'chunks' collection
            delayed
                :class:`~dask.delayed.Delayed`, or None if none of the variables use
                dask.
        """
        if isinstance(x, xarray.DataArray):
            x = x.to_dataset(name="__DataArray__")

        # MongoDB documents to be inserted in the 'chunks' collection
        chunks = []  # type: List[dict]

        # Building blocks of the dask graph
        keys = []  # type: List[tuple]
        layers = {}  # type: Dict[str, Dict[str, tuple]]
        dependencies = {}  # type: Dict[str, Set[str]]

        for var_name, variable in x.variables.items():
            if variable.__dask_graph__():
                put_keys, put_graph = self._delayed_put(
                    meta_id, str(var_name), variable
                )
                keys += put_keys
                layers.update(put_graph.layers)
                dependencies.update(put_graph.dependencies)
            else:
                chunks += chunk.array_to_docs(
                    # Variable.data, unlike Variable.values, preserves pint.Quantity
                    # and sparse.COO objects
                    variable.data,
                    meta_id=meta_id,
                    name=str(var_name),
                    chunk=None,
                    chunk_size_bytes=self.chunk_size_bytes,
                )

        if keys:
            # Build dask Delayed
            toplevel_name = "mongodb_put_dataset-" + dask.base.tokenize(*keys)
            layers[toplevel_name] = {toplevel_name: tuple((collect, *keys))}
            dependencies[toplevel_name] = {key[0] for key in keys}
            # Aggregate the delayeds into a single delayed
            graph = dask.highlevelgraph.HighLevelGraph(
                layers=layers, dependencies=dependencies
            )
            return chunks, Delayed(toplevel_name, graph)
        return chunks, None

    def _delayed_put(
        self, meta_id: bson.ObjectId, var_name: str, var: xarray.Variable
    ) -> Tuple[List[tuple], dask.highlevelgraph.HighLevelGraph]:
        """Helper method of put().
        Convert a dask-based Variable into a delayed put into the chunks collection.

        :returns: tuple of dask keys, layers, dependencies
        """
        coll: pymongo.collection.Collection = ensure_pymongo(self.chunks)
        name = "mongodb_put_array-" + dask.base.tokenize(coll, meta_id, var_name, var)
        dsk = {}
        keys = []

        for arr_k in dask.core.flatten(var.__dask_keys__()):
            put_k = (name,) + arr_k[1:]
            put_func = partial(
                chunk.mongodb_put_array,
                coll=coll,
                meta_id=meta_id,
                name=var_name,
                chunk=arr_k[1:],
                chunk_size_bytes=self.chunk_size_bytes,
            )
            dsk[put_k] = put_func, arr_k
            keys.append(put_k)

        graph = dask.highlevelgraph.HighLevelGraph.from_collections(
            name, dsk, dependencies=[var]
        )
        return keys, graph

    @staticmethod
    def _normalize_load(
        meta: dict, load: Union[bool, None, Collection[str]]
    ) -> Set[str]:
        """Helper method of get().
        Normalize the 'load' parameter of get().

        :param dict meta:
            document from the 'meta' collection
        :param load:
            True, False, None, or collection of variable names
            which may or may not exist. See
            :meth:`xarray_mongodb.XarrayMongoClient.get`.
        :returns:
            set of variable names to be loaded
        """
        variables = dict(meta["coords"])
        variables.update(meta["data_vars"])

        all_vars = set(variables.keys())
        index_coords = {k for k, v in variables.items() if v["dims"] == [k]}
        numpy_vars = {k for k, v in variables.items() if v["chunks"] is None}

        if load is True:
            return all_vars
        if load is False:
            return index_coords
        if load is None:
            return index_coords | numpy_vars
        if isinstance(load, Collection) and not isinstance(load, str):
            return index_coords | (all_vars & set(load))
        raise TypeError(load)

    @staticmethod
    def _chunks_query(meta: dict, load: Set[str]) -> dict:
        """Helper method of get().

        :param dict meta:
            document from the 'meta' collection
        :param set load:
            set of variable names that need to be loaded
            immediately
        :returns:
           find query for the 'chunk' collection
        """
        return {"meta_id": meta["_id"], "name": {"$in": list(load)}}

    def _docs_to_dataset(
        self, meta: dict, chunks: List[dict], load: Set[str]
    ) -> Union[xarray.DataArray, xarray.Dataset]:
        """Helper method of get().
        Convert a document dicts loaded from MongoDB to either a DataArray or a Dataset.
        """
        chunks.sort(key=lambda doc: (doc["name"], doc["chunk"], doc["n"]))

        # Convert list of docs into {var name: {chunk id: ndarray|COO|Quantity}}
        variables: DefaultDict[
            str, Dict[Optional[Tuple[int, ...]], EagerArray]
        ] = defaultdict(dict)

        for (var_name, chunk_id), docs in groupby(
            chunks, lambda doc: (doc["name"], doc["chunk"])
        ):
            if isinstance(chunk_id, list):
                chunk_id = tuple(chunk_id)
            array = chunk.docs_to_array(
                list(docs),
                {"meta_id": meta["_id"], "name": var_name, "chunk": chunk_id},
                ureg=self._ureg,
            )
            variables[var_name][chunk_id] = array

        def build_variables(where: str) -> Iterator[Tuple[str, xarray.Variable]]:
            for var_name, var_meta in meta[where].items():
                if var_name in load:
                    assert var_name in variables, var_name
                    array = self._build_eager_array(var_meta, variables[var_name])
                else:
                    assert var_name not in variables
                    array = self._build_dask_array(var_meta, var_name, meta["_id"])
                yield var_name, xarray.Variable(var_meta["dims"], array)

        ds = xarray.Dataset(
            coords=OrderedDict(build_variables("coords")),
            data_vars=OrderedDict(build_variables("data_vars")),
            attrs=OrderedDict(meta["attrs"]),
        )

        if meta["data_vars"].keys() == {"__DataArray__"}:
            da = cast(xarray.DataArray, ds["__DataArray__"])
            da.name = meta.get("name")
            return da
        return ds

    @staticmethod
    def _build_eager_array(
        meta: dict, chunks: Dict[Optional[Tuple[int, ...]], EagerArray]
    ) -> EagerArray:
        """Build an in-memory array (np.ndarray, sparse.COO, or pint.Quantity wrapping
        one of the previous two) from meta-data and chunks

        :param dict meta:
            sub-document from the 'meta' collection under the 'coords' or 'data_vars'
            key
        :param dict chunks:
            ``{chunk id: ndarray|COO|Quantity}``, where chunk id is a tuple e.g.
            ``(0, 0, 0)`` if the variable was backed by dask when it was stored, or None
            otherwise.
        """
        # Convert to ndarray
        if meta["chunks"] is None:
            assert len(chunks) == 1
            # chunks=None or single chunk
            return next(iter(chunks.values()))

        # variable was backed by dask and split in multiple chunks
        # at the moment of calling put()
        dsk = {
            ("stub",) + cast(tuple, chunk_id): array
            for chunk_id, array in chunks.items()
        }
        array = dask.array.Array(
            dsk,
            name="stub",
            shape=meta["shape"],
            chunks=chunks_lists_to_tuples(meta["chunks"]),
            dtype=meta["dtype"],
        )
        return array.compute(scheduler="sync")

    def _build_dask_array(
        self, meta: dict, name: str, meta_id: bson.ObjectId
    ) -> dask.array.Array:
        """Build a dask array from meta-data

        :param dict meta:
            sub-document from the 'meta' collection under the 'coords' or 'data_vars'
            key
        :param str name:
            variable name
        :param bson.ObjectId meta_id:
            Id of the document from the 'meta' collection
        """
        coll: pymongo.collection.Collection = ensure_pymongo(self.chunks)
        dsk_name = "mongodb_get_array-" + dask.base.tokenize(coll, meta_id, name)

        chunks: Union[tuple, int, float]
        if meta["chunks"] is None:
            chunks = -1
        else:
            chunks = chunks_lists_to_tuples(meta["chunks"])

        array = dask.array.Array(
            {}, name=dsk_name, shape=meta["shape"], chunks=chunks, dtype=meta["dtype"]
        )
        if hasattr(array, "_meta"):  # dask >= 2.0
            if "units" in meta:
                array._meta = self.ureg.Quantity(array._meta, meta["units"])

        for key in dask.core.flatten(array.__dask_keys__()):
            load_func = partial(
                chunk.mongodb_get_array,
                coll=coll,
                meta_id=meta_id,
                name=name,
                # Note: 'meta["chunks"] is not None' handles the special case of
                # dask array with shape=()
                chunk=key[1:] if meta["chunks"] is not None else None,
                ureg=self._ureg,
            )

            # See dask.highlevelgraph.HighLevelGraph
            array.dask.layers[dsk_name][key] = (load_func,)
        return array


def ensure_pymongo(obj):
    """If obj is wrapped by motor, return the internal pymongo object

    :param obj:
        a pymongo or motor client, database, or collection
    :returns:
        pymongo client, database, or collection
    """
    if obj.__class__.__module__.startswith("pymongo."):
        return obj
    if obj.delegate.__class__.__module__.startswith("pymongo."):
        return obj.delegate
    raise TypeError("Neither a pymongo nor a motor object")


def chunks_lists_to_tuples(level: Union[list, int, float]) -> Union[tuple, int, float]:
    """Convert a recursive list of lists of ints into a tuple of tuples of ints. This is
    a helper function needed because MongoDB automatically converts tuples to lists, but
    the dask constructor wants the chunks defined strictly as tuples.

    e.g.

    - input: ``[[1, 2], [3, 4]]``
    - output: ``((1, 2), (3, 4))``

    .. note::
       float data type is supported to allow for NaN-sized dask chunks
    """
    if isinstance(level, list):
        return tuple(chunks_lists_to_tuples(i) for i in level)
    if isinstance(level, (int, float)):
        return level
    raise TypeError(level)


def collect(*futures):
    """Dummy node in the dask graph whose only purpose is to collect a bunch of other
    nodes
    """
    pass
