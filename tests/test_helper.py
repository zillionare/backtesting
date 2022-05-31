import datetime
import unittest

import numpy as np

from backtest.common.helper import jsonify, tabulate_numpy_array


class HelperTest(unittest.TestCase):
    def test_jsonify(self):
        class Foo:
            def __init__(self):
                self.a = 1
                self.b = 2
                self.c = 3

        foo = Foo()
        obj = {
            "numpy": np.array([0.1, 0.2, 0.3]),
            "time": datetime.datetime(2020, 1, 1, 0, 0, 0),
            "list": [1, 2, 3],
            "dict": {"a": 1, "b": 2},
            "str": "hello",
            "bool": False,
            "None": None,
            "structure_array": np.array(
                [(1, 2, 3), (4, 5, 6)], dtype=[("a", int), ("b", int), ("c", int)]
            ),
            "foo": foo,
        }

        actual = jsonify(obj)
        exp = {
            "numpy": [0.1, 0.2, 0.3],
            "time": "2020-01-01T00:00:00",
            "list": [1, 2, 3],
            "dict": {"a": 1, "b": 2},
            "str": "hello",
            "bool": False,
            "None": None,
            "structure_array": [[1, 2, 3], [4, 5, 6]],
            "foo": {"a": 1, "b": 2, "c": 3},
        }

        self.assertDictEqual(actual, exp)

    def test_tabulate_numpy_array(self):
        arr = np.array(
            [(1, 2, 3), (4, 5, 6)], dtype=[("a", int), ("b", int), ("c", int)]
        )

        actual = tabulate_numpy_array(arr)
        exp = [["a", "b", "c"], [1, 2, 3], [4, 5, 6]]

        exp = """╒═════╤═════╤═════╕
│   a │   b │   c │
╞═════╪═════╪═════╡
│   1 │   2 │   3 │
├─────┼─────┼─────┤
│   4 │   5 │   6 │
╘═════╧═════╧═════╛"""
        self.assertEqual(exp, actual)
