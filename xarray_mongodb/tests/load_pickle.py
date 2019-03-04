"""Helper script of test_pickle.py, to be executed in a new interpreter
"""
import pickle
import sys

if __name__ == '__main__':
    with open(sys.argv[-1], 'rb') as fh:
        d = pickle.load(fh)
    d.compute()
