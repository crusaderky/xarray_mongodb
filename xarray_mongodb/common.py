"""Shared code between :class:`XarrayMongoDB` and :class:`XarrayMongoDBAsyncIO`
"""
from collections import OrderedDict, defaultdict
from functools import partial
from itertools import groupby
from typing import Union, List, Tuple, Set, Sequence

import bson
import dask.array
import dask.base
import dask.core
import dask.highlevelgraph
from dask.delayed import Delayed
import numpy
import pymongo
import xarray

from . import chunk


CHUNKS_INDEX = {'meta_id': 1, 'name': 1, 'chunk': 1}
CHUNKS_PROJECT = {'name': 1, 'chunk': 1, 'n': 1,
                  'dtype': 1, 'shape': 1, 'data': 1}
CHUNK_SIZE_BYTES_DEFAULT = 255 * 1024


@dask.base.normalize_token.register(pymongo.collection.Collection)
def _(obj: pymongo.collection.Collection):
    """Single-dispatch function.
    Make pymongo collections tokenizable with dask.
    """
    client = obj.database.client
    # See patch_pymongo.py
    return client.__args, client.__kwargs, obj.database.name, obj.name


def ensure_pymongo(obj):
    """If obj is wrapped by motor, return the internal pymongo object

    :param obj:
        a pymongo or motor client, database, or collection
    :returns:
        pymongo client, database, or collection
    """
    if obj.__class__.__module__.startswith('pymongo.'):
        return obj
    if not obj.delegate.__class__.__module__.startswith('pymongo.'):
        raise TypeError('Neither a pymongo nor a motor object')
    return obj.delegate


class XarrayMongoDBCommon:
    """Common functionality of :class:`XarrayMongoDB` and
    :class:`XarrayMongoDBAsyncIO`

    :param db:
        :class:`pymongo.database.Database` or
        :class:`motor.motor_asyncio.AsyncIOMotorDatabase`
    :param str collection:
        prefix of the collections to store the xarray data.
        Two collections will actually be created,
        <collection>.meta and <collection>.chunks.
    :param int chunk_size_bytes:
        Size of the payload in a document in the chunks collection.
        Not to be confused with dask chunks.
    """
    def __init__(self, db, collection: str = 'xarray',
                 chunk_size_bytes: int = CHUNK_SIZE_BYTES_DEFAULT):
        self.meta = db[collection].arrays
        self.chunks = db[collection].chunks
        self.chunk_size_bytes = chunk_size_bytes

    def _dataset_to_meta(self, x: Union[xarray.DataArray, xarray.Dataset]
                         ) -> dict:
        """Helper function of put().
        Convert a DataArray or Dataset into the dict
        to insert into the 'meta' collection
        """
        meta = {
            'type': x.__class__.__name__,
            'attrs': bson.SON(x.attrs),
            'coords': bson.SON(),
            'data_vars': bson.SON(),
            'chunkSize': self.chunk_size_bytes,
        }
        if isinstance(x, xarray.DataArray):
            x['name'] = x.name
            x = x._to_temp_dataset()

        for coll in ('coords', 'data_vars'):
            for k, v in getattr(x, coll).items():
                meta[coll][k] = {
                    'dims': v.dims,
                    'dtype': v.dtype.str,
                    'shape': v.shape,
                    'chunks': v.chunks,
                }
        return meta

    def _dataset_to_chunks(
            self, x: Union[xarray.DataArray, xarray.Dataset],
            meta_id: bson.ObjectId
    ) -> Tuple[List[dict], Union[Delayed, None]]:
        """Helper method of put().
        Convert a DataArray or Dataset into the list of dicts
        to insert into the 'chunks' collection. For dask variables,
        generate a :class:`~dask.delayed.Delayed` object.

        :param x:
            :class:`xarray.DataArray` or :class:`xarray.Dataset`
        :param bson.ObjectId meta_id:
            id_ of the document in the 'meta' collection
        :returns:
            tuple of

            chunks
                list of dicts to be inserted into the 'chunks' collection
            delayed
                :class:`~dask.delayed.Delayed`, or None if none of the
                variables use dask.
        """
        if isinstance(x, xarray.DataArray):
            x = x._to_temp_dataset()

        chunks = []
        delayeds = []

        for var_name, variable in x.variables.items():
            graph = variable.__dask_graph__()
            if graph:
                delayeds.append(self._delayed_put(
                    meta_id, var_name, variable))
            else:
                chunks += chunk.array_to_docs(
                    variable.values, meta_id=meta_id, name=var_name,
                    chunk=None, chunk_size_bytes=self.chunk_size_bytes)

        if delayeds:
            # Aggregate the delayeds into a single delayed
            name = 'mongodb_put_dataset-' + dask.base.tokenize(*delayeds)
            dsk = {name: tuple(d.name for d in delayeds)}
            graph = dask.highlevelgraph.HighLevelGraph.from_collections(
                name, dsk, dependencies=delayeds)
            return chunks, Delayed(name, graph)
        return chunks, None

    def _delayed_put(self, meta_id: bson.ObjectId, var_name: str,
                     var: xarray.Variable) -> Delayed:
        """Helper method of put().
        Convert a dask-based Variable into a delayed put into the
        chunks collection.
        """
        coll = ensure_pymongo(self.chunks)
        name = 'mongodb_put_array-' + dask.base.tokenize(
            coll, meta_id, var_name, var)
        dsk = {}

        for arr_k in dask.core.flatten(var.__dask_keys__()):
            put_k = (name,) + arr_k[1:]
            put_func = partial(
                chunk.mongodb_put_array,
                coll=coll, meta_id=meta_id, name=var_name, chunk=arr_k[1:],
                chunk_size_bytes=self.chunk_size_bytes)
            dsk[put_k] = put_func, arr_k

        dsk[name] = tuple(sorted(dsk.keys()))
        graph = dask.highlevelgraph.HighLevelGraph.from_collections(
            name, dsk, dependencies=[var])
        return Delayed(name, graph)

    @staticmethod
    def _normalize_load(meta: dict, load: Union[bool, None, Sequence[str]]
                        ) -> Set[str]:
        """Helper method of get().
        Normalize the 'load' parameter of get().

        :param dict meta:
            document from the 'meta' collection
        :param load:
            True, False, None, or sequence of variable names
            which may or may not exist
        :returns:
            set of variable names to be loaded
        """
        variables = dict(meta['coords'])
        variables |= meta['data_vars']

        all_vars = set(variables.keys())
        index_coords = {k for k, v in variables.items() if v['dims'] == [k]}
        numpy_vars = {k for k, v in variables.items() if v['chunks'] is None}

        if load is True:
            return all_vars
        if load is False:
            return index_coords
        if load is None:
            return index_coords | numpy_vars
        return index_coords | (all_vars & set(load))

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
        return {'meta_id': meta['_id'], 'name': {'$in': list(load)}}

    def _docs_to_dataset(self, meta: dict, chunks: List[dict],
                         load: Set[str]
                         ) -> Union[xarray.DataArray, xarray.Dataset]:
        """Helper method of get().
        Convert a document dicts loaded from MongoDB to either a DataArray
        or a Dataset.
        """
        chunks.sort(key=lambda doc: (doc['name'], doc['chunk'], doc['n']))

        # Convert list of docs into {var name: {chunk: numpy array}}
        variables = defaultdict(dict)
        for (var_name, chunk_id), docs in groupby(
                chunks, lambda doc: (doc['name'], doc['chunk'])):
            if isinstance(chunk_id, list):
                chunk_id = tuple(chunk_id)
            array = chunk.docs_to_array(
                list(docs),
                {'meta_id': meta['_id'], 'name': var_name, 'chunk': chunk_id})
            variables[var_name][chunk_id] = array

        def build_variables(where: str):
            for var_name, var_meta in meta[where].items():
                if var_name in load:
                    assert var_name in variables
                    array = self._build_numpy_array(
                        var_meta, variables[var_name])
                else:
                    assert var_name not in variables
                    array = self._build_dask_array(
                        var_meta, var_name, meta['_id'])
                yield var_name, xarray.Variable(var_meta['dims'], array)

        ds = xarray.Dataset(
            coords=OrderedDict(build_variables('coords')),
            data_vars=OrderedDict(build_variables('data_vars')),
            attrs=OrderedDict(meta['attrs']))

        if meta['type'] == 'DataArray':
            assert len(ds.data_vars) == 1
            ds = next(iter(ds.data_vars.values()))
            ds.name = meta['name']
        return ds

    @staticmethod
    def _build_numpy_array(meta: dict, chunks: dict) -> numpy.ndarray:
        """Build a numpy array from meta-data and chunks

        :param dict meta:
            sub-document from the 'meta' collection under the
            'coords' or 'data_vars' key
        :param dict chunks:
            ``{chunk id: numpy.ndarray}`` where chunk id is a tuple e.g.
            ``(0, 0, 0)`` if the variable was backed by dask when it was
            stored, or None if it was backed by numpy.
        """
        # Convert to numpy.ndarray
        if meta['chunks'] is None:
            assert len(chunks) == 1
            # chunks=None or single chunk
            return next(iter(chunks.values()))

        # variable was backed by dask and split in multiple chunks
        # at the moment of calling put()
        dsk = {
            ('stub',) + chunk_id: array
            for chunk_id, array in chunks.items()
        }
        array = dask.array.Array(
            dsk, name='stub', shape=meta['shape'],
            chunks=meta['chunks'], dtype=meta['dtype'])
        return array.compute(scheduler='sync')

    def _build_dask_array(self, meta: dict, name: str, meta_id: bson.ObjectId
                          ) -> dask.array.Array:
        """Build a dask array from meta-data

        :param dict meta:
            sub-document from the 'meta' collection under the
            'coords' or 'data_vars' key
        :param str name:
            variable name
        :param bson.ObjectId meta_id:
            Id of the document from the 'meta' collection
        """
        coll = ensure_pymongo(self.chunks)
        dsk_name = 'mongodb_get_array-' + dask.base.tokenize(
            coll, meta_id, name)

        array = dask.array.Array(
            {}, name=dsk_name, shape=meta['shape'],
            chunks=meta['chunks'] or -1, dtype=meta['dtype'])

        for key in dask.core.flatten(array.__dask_keys__()):
            load_func = partial(
                chunk.mongodb_get_array,
                coll=coll, meta_id=meta_id, name=name,
                chunk=key[1:] if meta['chunks'] is not None else None)

            # See dask.highlevelgraph.HighLevelGraph
            array.dask.layers[dsk_name][key] = load_func
        return array
