import bson
import pytest
import xarray
from xarray_mongodb import DocumentNotFoundError
from . import xdb  # noqa: F401


ds = xarray.Dataset(
    coords={
        'x': (('x', ), [1, 2]),
        'x2': (('x', ), [3, 4]),
        'x3': (('x', ), [5, 6]),
    },
    data_vars={
        'd': (('x', 'y'), [[10, 20], [30, 40]]),
        's': 1.0,
    },
    attrs={
        'foo': 'bar'
    })
ds['d'] = ds['d'].chunk({'x': 1, 'y': 2})
ds['x3'] = ds['x3'].chunk(1)


@pytest.mark.parametrize('compute,load,chunks', [  # noqa: F811
    (False, None, {'x': None, 'x2': None,
                   'x3': ((1, 1), ), 'd': ((1, 1, ), (2, )), 's': None}),
    (False, False, {'x': None, 'x2': ((2, ), ),
                    'x3': ((1, 1), ), 'd': ((1, 1, ), (2, )), 's': ()}),
    (False, True, {'x': None, 'x2': None, 'x3': None, 'd': None, 's': None}),
    (False, ['d'], {'x': None, 'x2': ((2, ), ), 'x3': ((1, 1), ), 'd': None,
                    's': ()}),
    (True, None, {'x': None, 'x2': None, 'x3': None, 'd': None, 's': None}),
    (True, False, {'x': None, 'x2': ((2,),),
                   'x3': ((2, ),), 'd': ((2, ), (2, )), 's': ()}),
    (True, True, {'x': None, 'x2': None, 'x3': None, 'd': None, 's': None}),
    (True, ['d'], {'x': None, 'x2': ((2,),), 'x3': ((2, ),), 'd': None,
                   's': ()}),
])
def test_roundtrip(xdb, compute, load, chunks):
    if compute:
        _id, future = xdb.put(ds.compute())
        assert future is None
    else:
        _id, future = xdb.put(ds)
        future.compute()
    assert isinstance(_id, bson.ObjectId)

    ds2 = xdb.get(_id, load=load)
    xarray.testing.assert_identical(ds, ds2)
    assert {
        k: v.chunks for k, v in ds2.variables.items()
    } == chunks


def test_db_contents(xdb):  # noqa: F811
    assert xdb.meta.name == 'xarray.arrays'
    assert xdb.chunks.name == 'xarray.chunks'

    _id, future = xdb.put(ds)
    future.compute()

    expect_meta = [{
        '_id': _id,
        'attrs': {'foo': 'bar'},
        'chunkSize': 261120,
        'coords': {
            'x': {'chunks': None, 'dims': ['x'], 'dtype': '<i8', 'shape': [2]},
            'x2': {'chunks': None, 'dims': ['x'], 'dtype': '<i8',
                   'shape': [2]},
            'x3': {'chunks': [[1, 1]], 'dims': ['x'], 'dtype': '<i8',
                   'shape': [2]}
        },
        'data_vars': {
            'd': {'chunks': [[1, 1], [2]], 'dims': ['x', 'y'],
                  'dtype': '<i8', 'shape': [2, 2]},
            's': {'chunks': None, 'dims': [], 'dtype': '<f8', 'shape': []}
        }
    }]

    expect_chunks = [
     {'chunk': [0, 0],  # noqa: E121
      'data': b'\n\x00\x00\x00\x00\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00'
              b'\x00',
      'dtype': '<i8',
      'meta_id': _id,
      'n': 0,
      'name': 'd',
      'shape': [1, 2]},
     {'chunk': [1, 0],
      'data': b'\x1e\x00\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00',
      'dtype': '<i8',
      'meta_id': _id,
      'n': 0,
      'name': 'd',
      'shape': [1, 2]},
     {'chunk': None,
      'data': b'\x00\x00\x00\x00\x00\x00\xf0?',
      'dtype': '<f8',
      'meta_id': _id,
      'n': 0,
      'name': 's',
      'shape': []},
     {'chunk': None,
      'data': b'\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00'
              b'\x00\x00',
      'dtype': '<i8',
      'meta_id': _id,
      'n': 0,
      'name': 'x',
      'shape': [2]},
     {'chunk': None,
      'data': b'\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00'
              b'\x00\x00',
      'dtype': '<i8',
      'meta_id': _id,
      'n': 0,
      'name': 'x2',
      'shape': [2]},
     {'chunk': [0],
      'data': b'\x05\x00\x00\x00\x00\x00\x00\x00',
      'dtype': '<i8',
      'meta_id': _id,
      'n': 0,
      'name': 'x3',
      'shape': [1]},
     {'chunk': [1],
      'data': b'\x06\x00\x00\x00\x00\x00\x00\x00',
      'dtype': '<i8',
      'meta_id': _id,
      'n': 0,
      'name': 'x3',
      'shape': [1]}
    ]

    assert list(xdb.meta.find()) == expect_meta
    chunks = sorted(
        xdb.chunks.find({}, {'_id': False}),
        key=lambda doc: (doc['name'], doc['chunk']))
    assert chunks == sorted(
        expect_chunks,
        key=lambda doc: (doc['name'], doc['chunk']))


def test_dataarray(xdb):  # noqa: F811
    a = xarray.DataArray([1, 2], dims=['x'], coords={'x': ['x1', 'x2']})
    _id, _ = xdb.put(a)
    a2 = xdb.get(_id)
    xarray.testing.assert_identical(a, a2)


def test_multisegment(xdb):  # noqa: F811
    xdb.chunk_size_bytes = 4
    _id, future = xdb.put(ds)
    future.compute()
    assert xdb.chunks.find_one({'n': 2})
    ds2 = xdb.get(_id)
    xarray.testing.assert_identical(ds, ds2)


def test_meta_not_found(xdb):  # noqa: F811
    with pytest.raises(DocumentNotFoundError) as ex:
        xdb.get(bson.ObjectId('deadbeefdeadbeefdeadbeef'))
    assert str(ex.value) == 'deadbeefdeadbeefdeadbeef'


def test_no_segments_found(xdb):  # noqa: F811
    _id, future = xdb.put(ds)
    future.compute()
    xdb.chunks.delete_many({'name': 'd', 'chunk': [1, 0]})
    ds2 = xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}")


# A missing chunk with chunk_size_bytes=2 causes np.frombuffer to crash
# with 'ValueError: buffer size must be a multiple of element size'
# A missing chunk with chunk_size_bytes=8 causes ndarray.reshape
# to crash with 'ValueError: cannot reshape array of size 1 into shape (1,2)'
@pytest.mark.parametrize('chunk_size_bytes', (2, 8))  # noqa: F811
def test_some_segments_not_found(xdb, chunk_size_bytes):
    xdb.chunk_size_bytes = chunk_size_bytes
    _id, future = xdb.put(ds)
    future.compute()
    xdb.chunks.delete_one({'name': 'd', 'chunk': [1, 0], 'n': 1})
    ds2 = xdb.get(_id)
    with pytest.raises(DocumentNotFoundError) as ex:
        ds2.compute()
    assert str(ex.value) == (
        f"{{'meta_id': ObjectId('{_id}'), 'name': 'd', 'chunk': (1, 0)}}")
