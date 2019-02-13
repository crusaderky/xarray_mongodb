"""Shared code between :class:`XarrayMongoDB` and :class:`XarrayMongoDBAsyncIO`
"""
from collections import OrderedDict, defaultdict
from functools import partial
from itertools import groupby
from typing import Union, Sequence, Set

from xarray import DataArray, Dataset, Variable
from bson.objectid import ObjectId
from dask.array import Array
from dask.base import tokenize
from dask.core import flatten
from dask.delayed import Delayed
from dask.highlevelgraph import HighLevelGraph
from . import chunk

CHUNKS_INDEX = {'meta_id': 1, 'name': 1, 'chunk': 1}
CHUNKS_PROJECT = {'name': 1, 'chunk': 1, 'n': 1,
                  'dtype': 1, 'shape': 1, 'data': 1}
CHUNK_SIZE_BYTES_DEFAULT = 255 * 1024


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

    def __init__(self, db,
                 collection: str = 'xarray',
                 chunk_size_bytes: int = CHUNK_SIZE_BYTES_DEFAULT):
        self.meta = db[collection].arrays
        self.chunks = db[collection].chunks
        self.chunk_size_bytes = chunk_size_bytes

    def _dataset_to_meta(self, x: Union[DataArray, Dataset]) -> dict:
        """Helper function of put().
        Convert a DataArray or Dataset into the dict
        to insert into the 'meta' collection
        """
        meta = {
            'attrs': dict(x.attrs),
            'attrsOrder': list(x.attrs.keys()),
            'name': getattr(x, 'name', None),
            'variables': {},
            'coords': list(x.coords.keys()),
            'chunkSize': self.chunk_size_bytes,
        }
        if isinstance(x, DataArray):
            x = x._to_temp_dataset()
            meta['type'] = 'DataArray'
        else:
            meta['type'] = 'Dataset'
        meta['data_vars'] = list(x.data_vars.keys())

        for k, v in x.variables.items():
            meta['variables'][k] = {
                'dims': v.dims,
                'dtype': v.dtype.str,
                'shape': v.shape,
                'chunks': v.chunks,
            }
        return meta

    def _dataset_to_chunks(self, x: Union[DataArray, Dataset], id_: ObjectId):
        """Helper method of put().
        Convert a DataArray or Dataset into the list of dicts
        to insert into the 'chunks' collection. For dask variables,
        generate a :class:`~dask.delayed.Delayed` object.

        :param x:
            :class:`xarray.DataArray` or :class:`xarray.Dataset`
        :param ObjectId id_:
            id_ of the document in the 'meta' collection
        :param int chunk_size_bytes:
            maximum size of the data buffer on the MongoDB 'chunks'
            collection. Not to be confused with dask chunks.
        :returns:
            tuple of

            chunks
                list of dicts to be inserted into the 'chunks' collection
            delayed
                :class:`~dask.delayed.Delayed`, or None if none of the
                variables use dask.
        """
        if isinstance(x, DataArray):
            x = x._to_temp_dataset()

        chunks = []
        delayeds = []

        for var_name, variable in x.variables.items():
            graph = variable.__dask_graph__()
            if graph:
                delayeds.append(self._delayed_put(
                    id_, var_name, variable.data))
            else:
                chunks += chunk.array_to_docs(
                    variable.values, meta_id=id_, name=var_name, chunk=None,
                    chunk_size_bytes=self.chunk_size_bytes)

        if delayeds:
            name = 'mongodb_put_dataset-' + tokenize(delayeds)
            dsk = {name: tuple(d.name for d in delayeds)}
            graph = HighLevelGraph.from_collections(
                name, dsk, dependencies=delayeds)
            return chunks, Delayed(name, graph)
        return chunks, None

    def _delayed_put(self, meta_id: ObjectId, var_name: str,
                     array: Array) -> Delayed:
        """Helper method of put().
        Convert a dask Variable into a delayed put into the
        chunks collection.
        """
        name = 'mongodb_put_array-' + tokenize(array)
        dsk = {}
        for arr_k in flatten(array.__dask_keys__()):
            put_k = (name,) + arr_k[1:]
            put_func = partial(
                chunk.mongodb_put_array,
                address=self.chunks.db.client.address,
                db=self.chunks.db.name,
                collection=self.chunks.name,
                meta_id=meta_id,
                name=var_name,
                chunk=arr_k[1:],
                chunk_size_bytes=self.chunk_size_bytes)
            dsk[put_k] = put_func, arr_k
        dsk[name] = tuple(sorted(dsk.keys()))
        graph = HighLevelGraph.from_collections(
            name, dsk, dependencies=[array])
        return Delayed(name, graph)

    @staticmethod
    def _normalize_load(meta: dict, load: Union[bool, None, Sequence[str]]
                        ) -> set:
        """Helper function of get().
        Normalize the 'load' parameter of get().

        :param dict meta:
            document from the 'meta' collection
        :param load:
            True, False, None, or sequence of variable names
            which may or may not exist
        :returns:
            set of variable names to be loaded
        """
        all_vars = set(meta['variables'].keys())
        index_coords = {
            k for k, v in meta['variables'].items()
            if v['dims'] == [k]
        }
        numpy_vars = {
            k for k, v in meta['variables'].items()
            if v['chunks'] is None
        }

        if load is True:
            return all_vars
        if load is False:
            return index_coords
        if load is None:
            return index_coords | numpy_vars
        return index_coords | (all_vars & set(load))

    @staticmethod
    def _chunks_query(meta: dict, load: Set[str]) -> dict:
        """Helper function of get().

        :param dict meta:
            document from the 'meta' collection
        :param set load:
            set of variable names that need to be loaded
            immediately
        :returns:
           find query for the 'chunk' collection
        """
        return {'meta_id': meta['_id'], 'name': {'$in': list(load)}}

    def _docs_to_dataset(self, meta: dict, chunks: Sequence[dict],
                         load: Set[str]
                         ) -> Union[DataArray, Dataset]:
        """Convert a document dicts loaded from MongoDB to either a DataArray
        or a Dataset
        """
        chunks.sort(lambda doc: (doc['name'], doc['chunk'], doc['n']))

        # Convert list of docs into {var name: {chunk: numpy array}}
        variables = defaultdict(dict)
        for (name, chunk_id), docs in groupby(
                chunks, lambda doc: (doc['name'], doc['chunk'])):
            if isinstance(chunk_id, list):
                chunk_id = tuple(chunk_id)
            array = chunk.docs_to_array(
                list(docs),
                {'meta_id': meta['_id'], 'name': name, 'chunk': chunk_id})
            variables[name][chunk_id] = array

        # Convert {chunk id: ndarray} to xarray.Variable
        for var_name, var_meta in meta['variables'].items():
            if var_name in load:
                chunks_dict = variables[var_name]
                # Convert to numpy.ndarray
                if var_meta['chunks'] is None:
                    assert len(chunks_dict) == 1
                    # chunks=None or single chunk
                    array = next(iter(chunks_dict.values()))
                else:
                    # variable was backed by dask and split in multiple chunks
                    # at the moment of calling put()
                    dsk = {
                        ('stub',) + chunk_id: array
                        for chunk_id, array in chunks_dict.items()
                    }
                    array = Array(dsk, name='stub', shape=var_meta['shape'],
                                  chunks=var_meta['chunks'],
                                  dtype=var_meta['dtype'])
                    array = array.compute(scheduler='sync')
            else:
                assert var_name not in variables
                # Convert to dask.array.Array
                dsk_name = 'mongodb_get_array-' + tokenize(
                    address=self.chunks.db.client.address,
                    db=self.chunks.db.name,
                    collection=self.chunks.name,
                    meta_id=meta['_id'],
                    name=var_name)
                array = Array({}, name=dsk_name, shape=var_meta['shape'],
                              chunks=var_meta['chunks'] or -1,
                              dtype=var_meta['dtype'])
                for key in flatten(array.__dask_keys__()):
                    load_func = partial(
                        chunk.mongodb_get_array,
                        address=self.chunks.db.client.address,
                        db=self.chunks.db.name,
                        collection=self.chunks.name,
                        meta_id=meta['_id'],
                        name=var_name,
                        chunk=key[1:])
                    # See dask.highlevelgraph.HighLevelGraph
                    array.dask.layers[dsk_name][key] = load_func

            variables[var_name] = Variable(var_meta['dims'], array)

        ds = Dataset(
            coords=OrderedDict(
                (k, variables[k]) for k in meta['coords']),
            data_vars=OrderedDict(
                (k, variables[k]) for k in meta['data_vars']),
            attrs=OrderedDict(
                (k, meta['attrs'][k]) for k in meta['attrsOrder']))

        if meta['type'] == 'DataArray':
            assert len(ds.data_vars) == 1
            ds = next(iter(ds.data_vars.values()))
            ds.name = meta['name']
        return ds
