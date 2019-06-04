"""Helper script of test_pickle.py, to be executed in a new interpreter
"""
import os
import pickle
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


if __name__ == "__main__":
    with open(sys.argv[-1], "rb") as fh:
        d = pickle.load(fh)
    d.compute()
