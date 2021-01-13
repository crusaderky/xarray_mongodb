"""Low level functions:

- loading/writing a numpy.ndarray on MongoDB
- converting between MongoDB documnents and numpy.ndarray
"""
from typing import List, Optional, Tuple

import numpy as np
import pymongo
from bson import ObjectId

from .errors import DocumentNotFoundError


def mongodb_put_array(
    array: np.ndarray,
    coll: pymongo.collection.Collection,
    meta_id: ObjectId,
    name: str,
    chunk: tuple,
    chunk_size_bytes: int,
) -> None:
    """Insert a single chunk into MongoDB"""
    docs = array_to_docs(
        array,
        meta_id=meta_id,
        name=name,
        chunk=chunk,
        chunk_size_bytes=chunk_size_bytes,
    )
    assert docs
    coll.insert_many(docs)


def mongodb_get_array(
    coll: pymongo.collection.Collection,
    meta_id: ObjectId,
    name: str,
    chunk: Optional[Tuple[int, ...]],
) -> np.ndarray:
    """Load all MongoDB documents making up a dask chunk and assemble them into
    an array
    """
    find_key = {"meta_id": meta_id, "name": name, "chunk": chunk}
    docs = list(coll.find(find_key, {"dtype": 1, "shape": 1, "data": 1}).sort("n"))
    return docs_to_array(docs, find_key)


def array_to_docs(
    array: np.ndarray,
    meta_id: ObjectId,
    name: str,
    chunk: Optional[Tuple[int, ...]],
    chunk_size_bytes: int,
) -> List[dict]:
    """Convert a numpy array to a list of MongoDB documents ready to be inserted into
    the 'chunks' collection
    """
    if not isinstance(array, np.ndarray):
        raise TypeError(f"Unsupported xarray backend: {type(array)}")

    buffer = array.tobytes()
    # Guarantee at least one document in case of size 0
    buflen = max(len(buffer), 1)
    return [
        {
            "meta_id": meta_id,
            "name": name,
            "chunk": chunk,
            "n": n,
            "dtype": array.dtype.str,
            "shape": array.shape,
            "type": "ndarray",
            "data": buffer[offset : offset + chunk_size_bytes],
        }
        for n, offset in enumerate(range(0, buflen, chunk_size_bytes))
    ]


def docs_to_array(docs: List[dict], find_key: dict) -> np.ndarray:
    """Convert a list of MongoDB documents from the 'chunks' collection into a numpy
    array.

    :param list docs:
        MongoDB documents. Must be already sorted by 'n'.
    :param dict find_key:
        tag to use when raising DocumentNotFoundError
    :raises DocumentNotFoundError:
        No documents, or one or more documents are missing
    """
    if not docs:
        raise DocumentNotFoundError(find_key)
    buffer = b"".join([doc["data"] for doc in docs])
    dtype = docs[0]["dtype"]
    shape = docs[0]["shape"]

    # In case of a missing chunk,
    # - if bytes_per_chunk is not an exact multiple of dtype.size, np.frombuffer crashes
    #   with 'ValueError: buffer size must be a multiple of element size'
    # - if bytes_per_chunk is an exact multiple of dtype.size, ndarray.reshape crashes
    #   with 'ValueError: cannot reshape array of size 1 into shape (1,2)'
    try:
        return np.frombuffer(buffer, dtype).reshape(shape)
    except ValueError as e:
        # Missing some chunks
        raise DocumentNotFoundError(find_key) from e
