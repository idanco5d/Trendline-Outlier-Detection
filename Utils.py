from bisect import bisect
from collections import defaultdict
from typing import List, Hashable, DefaultDict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


def getAggregatedColumn(dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> pd.Series:
    return dataFrame.iloc[:, aggregationAttributeIndex]


def getListOfColumnValues(dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> List[float]:
    return list(getAggregatedColumn(dataFrame, aggregationAttributeIndex).unique())


def emptyDataFrame(baseDfColumns) -> pd.DataFrame:
    return pd.DataFrame(columns=baseDfColumns)


def listOfEmptyDictionaries(outputListLength: int, columnBase) -> List[DefaultDict[float, pd.DataFrame]]:
    return [createBinarySearchDefaultDict(columnBase) for _ in range(outputListLength)]


def createBinarySearchDefaultDict(column_base):
    class BinarySearchDefaultDict(defaultdict):
        def __init__(self, *args, **kwargs):
            super().__init__(lambda: pd.DataFrame(columns=column_base), *args, **kwargs)
            self._sorted_keys = []

        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            # Maintain sorted keys for binary search
            idx = bisect(self._sorted_keys, key)
            self._sorted_keys.insert(idx, key)

        def __missing__(self, key):
            # Binary search for the largest key less than or equal to the requested key
            idx = bisect(self._sorted_keys, key) - 1

            if idx >= 0:
                return self[self._sorted_keys[idx]]

            # Default empty DataFrame
            return self.default_factory()

    return BinarySearchDefaultDict()


def getGroupByKey(groupedRows: DataFrameGroupBy, key: Hashable) -> pd.DataFrame:
    firstKey = next(iter(groupedRows.groups))
    if isinstance(firstKey, tuple):
        return groupedRows.get_group(key)
    return groupedRows.get_group((key,))


def dataFramesUnion(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    if df1.empty:
        return df2
    if df2.empty:
        return df1
    return pd.concat([df1, df2], ignore_index=False)


def calculateRemovedTuples(originalDataFrame: pd.DataFrame, containedDataFrame: pd.DataFrame) -> pd.DataFrame:
    mergedDataFrame = originalDataFrame.merge(containedDataFrame, how='outer', indicator=True)
    differenceDf = mergedDataFrame[mergedDataFrame['_merge'] == 'left_only']

    return differenceDf.drop(columns=['_merge'])
