"""Shared code between :class:`XarrayMongoDB` and :class:`XarrayMongoDBAsyncIO`
"""
from collections import defaultdict
from functools import partial
from itertools import groupby
from typing import (
    AbstractSet,
    Collection,
    DefaultDict,
    Dict,
    Hashable,
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
import numpy as np
import pymongo
import xarray
from dask.delayed import Delayed

from . import chunk
from .nep18 import Quantity, UnitRegistry

CHUNKS_INDEX = [("meta_id", 1), ("name", 1), ("chunk", 1)]
CHUNK_SIZE_BYTES_DEFAULT = 255 * 1024
EMBED_THRESHOLD_BYTES_DEFAULT = 255 * 1024


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
        See :class:`~xarray_mongodb.XarrayMongoDB`
    :param int chunk_size_bytes:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    :param int embed_threshold_bytes:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    :param pint.registry.UnitRegistry ureg:
        See :class:`~xarray_mongodb.XarrayMongoDB`
    """

    chunk_size_bytes: int
    embed_threshold_bytes: int
    _ureg: Optional[UnitRegistry]
    _has_index: bool

    def __init__(
        self,
        database,
        collection: str,
        *,
        chunk_size_bytes: int,
        embed_threshold_bytes: int,
        ureg: Optional[UnitRegistry],
    ):
        self.meta = database[collection].meta
        self.chunks = database[collection].chunks
        self.chunk_size_bytes = chunk_size_bytes
        self.embed_threshold_bytes = embed_threshold_bytes
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
        if self._ureg:
            return self._ureg

        import pint

        return pint.get_application_registry()

    @ureg.setter
    def ureg(self, value: Optional[UnitRegistry]) -> None:
        self._ureg = value

    def _dataset_to_meta(
        self, x: Union[xarray.DataArray, xarray.Dataset]
    ) -> Tuple[dict, Dict[Hashable, Union[np.ndarray, dask.array.Array]]]:
        """Helper function of put().
        Convert a DataArray or Dataset into the dict to insert into the 'meta'
        collection.

        :returns:
            - meta document
            - raw variable data to be inserted into the 'chunks' collection
        """
        meta = {
            "coords": bson.SON(),
            "data_vars": bson.SON(),
            "chunkSize": self.chunk_size_bytes,
        }
        if x.attrs:
            meta["attrs"] = bson.SON(x.attrs)

        if isinstance(x, xarray.DataArray):
            if x.name:
                meta["name"] = x.name
            x = x.to_dataset(name="__DataArray__")

        # Tuples (size in bytes, variable name, subdocument)
        embed_candidates: List[Tuple[int, Hashable, dict]] = []
        variables_data: Dict[Hashable, Union[np.ndarray, dask.array.Array]] = {}

        for k, v in x.variables.items():
            subdoc = meta["coords"] if k in x.coords else meta["data_vars"]
            subdoc[k] = {
                "dims": v.dims,
                "dtype": v.dtype.str,
                "shape": v.shape,
                "chunks": v.chunks,
                "type": "ndarray",
            }
            if v.attrs and k != "__DataArray__":
                subdoc[k]["attrs"] = v.attrs
            data = v.data
            if isinstance(data, Quantity):
                subdoc[k]["units"] = str(data.units)
                data = data.magnitude
            if isinstance(data, np.number):
                data = np.asarray(data)
            variables_data[k] = data
            if isinstance(data, np.ndarray):
                embed_candidates.append((data.dtype.itemsize * data.size, k, subdoc))
            elif not isinstance(data, dask.array.Array):
                # Never embed dask variables
                raise TypeError(f"Unsupported xarray backend: {type(data)}")

        # Embed variables, starting with the smallest ones, until we hit
        # embed_threshold_bytes
        embed_candidates.sort(key=lambda t: t[0])
        avail_bytes = self.embed_threshold_bytes
        for size, k, subdoc in embed_candidates:
            avail_bytes -= size
            if avail_bytes < 0:
                break
            subdoc[k]["data"] = variables_data.pop(k).tobytes()

        return meta, variables_data

    def _dataset_to_chunks(
        self,
        variables_data: Dict[Hashable, Union[np.ndarray, dask.array.Array]],
        meta_id: bson.ObjectId,
    ) -> Tuple[List[dict], Optional[Delayed]]:
        """Helper method of put().
        Convert a DataArray or Dataset into the list of dicts to insert into the
        'chunks' collection. For dask variables, generate a
        :class:`~dask.delayed.Delayed` object.

        :param variables_data:
            raw variables to be inserted into the 'chunks' collection
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
        # MongoDB documents to be inserted in the 'chunks' collection
        chunks = []  # type: List[dict]

        # Building blocks of the dask graph
        keys = []  # type: List[tuple]
        layers = {}  # type: Dict[str, Dict[str, tuple]]
        dependencies = {}  # type: Dict[str, Set[str]]

        for var_name, data in variables_data.items():
            if isinstance(data, dask.array.Array):
                put_keys, put_graph = self._delayed_put(meta_id, str(var_name), data)
                keys += put_keys
                layers.update(put_graph.layers)
                dependencies.update(put_graph.dependencies)
            elif isinstance(data, np.ndarray):
                chunks += chunk.array_to_docs(
                    # Variable.data, unlike Variable.values, preserves pint.Quantity
                    # and sparse.COO objects
                    data,
                    meta_id=meta_id,
                    name=str(var_name),
                    chunk=None,
                    chunk_size_bytes=self.chunk_size_bytes,
                )
            else:
                raise AssertionError("unreachable")  # pragma: nocover

        if not keys:
            return chunks, None

        # Build dask Delayed
        toplevel_name = "mongodb_put_dataset-" + dask.base.tokenize(*keys)
        layers[toplevel_name] = {toplevel_name: tuple((collect, *keys))}
        dependencies[toplevel_name] = {key[0] for key in keys}
        # Aggregate the delayeds into a single delayed
        graph = dask.highlevelgraph.HighLevelGraph(
            layers=layers, dependencies=dependencies
        )
        return chunks, Delayed(toplevel_name, graph)

    def _delayed_put(
        self, meta_id: bson.ObjectId, var_name: str, data: dask.array.Array
    ) -> Tuple[List[tuple], dask.highlevelgraph.HighLevelGraph]:
        """Helper method of put().
        Convert a dask-based Variable into a delayed put into the chunks collection.

        :returns: tuple of dask keys, layers, dependencies
        """
        coll: pymongo.collection.Collection = ensure_pymongo(self.chunks)
        name = "mongodb_put_array-" + dask.base.tokenize(coll, meta_id, var_name, data)
        dsk = {}
        keys = []

        for arr_k in dask.core.flatten(data.__dask_keys__()):
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
            name, dsk, dependencies=[data]
        )
        return keys, graph

    @staticmethod
    def _prepare_get(
        meta: dict, load: Union[bool, None, Collection[Hashable]]
    ) -> Tuple[AbstractSet[str], Optional[dict]]:
        """Helper method of get().
        Normalize the 'load' parameter of get() and build a query for the 'chunks'
        collection.

        :param dict meta:
            document from the 'meta' collection
        :param load:
            True, False, None, or collection of variable names
            which may or may not exist. See
            :meth:`xarray_mongodb.XarrayMongoClient.get`.
        :returns:
            - Set of variable names expected from the 'chunks' collection
            - Query for the 'chunks' collection, or None if all variables are embedded
        """
        variables = dict(meta["coords"])
        variables.update(meta["data_vars"])

        index_coords = {k for k, v in variables.items() if v["dims"] == [k]}
        non_dask_vars = {k for k, v in variables.items() if v["chunks"] is None}
        embedded_vars = {k for k, v in variables.items() if "data" in v}

        if load is True:
            load_norm: AbstractSet[str] = variables.keys()
        elif load is False:
            load_norm = index_coords
        elif load is None:
            load_norm = index_coords | non_dask_vars
        elif isinstance(load, Collection) and not isinstance(load, str):
            load_norm = index_coords | (variables.keys() & {str(k) for k in load})
        else:
            raise TypeError(load)
        load_norm = load_norm - embedded_vars

        if not load_norm:
            return load_norm, None

        query = {"meta_id": meta["_id"]}
        if variables.keys() - embedded_vars - load_norm:
            query["name"] = {"$in": list(load_norm)}
        return load_norm, query

    def _docs_to_dataset(
        self, meta: dict, chunks: List[dict], load: AbstractSet[str]
    ) -> Union[xarray.DataArray, xarray.Dataset]:
        """Helper method of get().
        Convert a document dicts loaded from MongoDB to either a DataArray or a Dataset.
        """
        chunks.sort(key=lambda doc: (doc["name"], doc["chunk"], doc["n"]))

        # Convert list of docs into {var name: {chunk id: ndarray|COO|Quantity}}
        variables: DefaultDict[
            str, Dict[Optional[Tuple[int, ...]], np.ndarray]
        ] = defaultdict(dict)

        for (var_name, chunk_id), docs in groupby(
            chunks, lambda doc: (doc["name"], doc["chunk"])
        ):
            if isinstance(chunk_id, list):
                chunk_id = tuple(chunk_id)
            array = chunk.docs_to_array(
                list(docs),
                {"meta_id": meta["_id"], "name": var_name, "chunk": chunk_id},
            )
            variables[var_name][chunk_id] = array

        def build_variables(where: str) -> Iterator[Tuple[str, xarray.Variable]]:
            for var_name, var_meta in meta[where].items():
                if "data" in var_meta:
                    # Embedded variable
                    assert var_name not in variables
                    array = self._build_eager_array(
                        var_meta, {None: chunk.docs_to_array([var_meta], {})}
                    )
                elif var_name in load:
                    assert var_name in variables
                    array = self._build_eager_array(var_meta, variables[var_name])
                else:
                    assert var_name not in variables
                    array = self._build_dask_array(var_meta, var_name, meta["_id"])

                if "units" in var_meta:
                    # wrap numpy/dask array with pint
                    array = self.ureg.Quantity(array, var_meta["units"])

                yield var_name, xarray.Variable(
                    var_meta["dims"], array, attrs=var_meta.get("attrs")
                )

        ds = xarray.Dataset(
            coords=dict(build_variables("coords")),
            data_vars=dict(build_variables("data_vars")),
            attrs=meta.get("attrs", {}),
        )

        if meta["data_vars"].keys() == {"__DataArray__"}:
            da = cast(xarray.DataArray, ds["__DataArray__"])
            da.attrs = ds.attrs
            da.name = meta.get("name")
            return da
        return ds

    @staticmethod
    def _build_eager_array(
        meta: dict, chunks: Dict[Optional[Tuple[int, ...]], np.ndarray]
    ) -> np.ndarray:
        """Build an in-memory array (np.ndarray or sparse.COO) from meta-data and chunks

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
        """Build a dask array with np.ndarray or sparse.COO chunks from metadata

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
        dsk = {}
        for key in dask.core.flatten(array.__dask_keys__()):
            load_func = partial(
                chunk.mongodb_get_array,
                coll=coll,
                meta_id=meta_id,
                name=name,
                # Note: 'meta["chunks"] is not None' handles the special case of
                # dask array with shape=()
                chunk=key[1:] if meta["chunks"] is not None else None,
            )
            dsk[key] = (load_func,)
        array.dask = dsk
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
