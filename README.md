xarray_mongodb
==============
Store xarray objects in MongoDB


To generate the documentation
-----------------------------
1. Source conda environment
2. Move to the root directory of this project
3. 
```bash
conda env create -n xarray_mongodb_docs --file ci/requirements-docs.yml
conda activate xarray_mongodb_docs
export PYTHONPATH=$PWD
sphinx-build -n -j auto -b html -d build/doctrees doc build/html
```

To run the tests
----------------
1. Start MongoDB on localhost (no password)
2. Source conda environment
3. Move to the root directory of this project
4. 
```bash
conda env create -n xarray_mongodb_py37 --file ci/requirements-py37.yml
conda activate xarray_mongodb_py37
export PYTHONPATH=$PWD
py.test
```
Replace py37 with any of the environments available in the ``ci`` directory.