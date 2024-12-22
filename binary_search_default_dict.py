from bisect import bisect
from collections import defaultdict

import pandas as pd

# TODO try and see if this can be sorted once at the end


class BinarySearchDefaultDict(defaultdict):
    def __init__(self, columnBase, *args, **kwargs):
        super().__init__(lambda: pd.DataFrame(columns=columnBase), *args, **kwargs)
        self._sorted_keys = []

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        idx = bisect(self._sorted_keys, key)
        self._sorted_keys.insert(idx, key)

    def __missing__(self, key):
        idx = bisect(self._sorted_keys, key) - 1

        if idx >= 0:
            return self[self._sorted_keys[idx]]

        return self.default_factory()
