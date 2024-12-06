from typing import List, Hashable, DefaultDict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from BinarySearchDefaultDict import BinarySearchDefaultDict


def getAggregatedColumn(dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> pd.Series:
    return dataFrame.iloc[:, aggregationAttributeIndex]


def getListOfColumnValues(dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> List[float]:
    return list(getAggregatedColumn(dataFrame, aggregationAttributeIndex).unique())


def emptyDataFrame(baseDfColumns) -> pd.DataFrame:
    return pd.DataFrame(columns=baseDfColumns)


def listOfEmptyDictionaries(outputListLength: int, columnBase) -> List[DefaultDict[float, pd.DataFrame]]:
    return [BinarySearchDefaultDict(columnBase) for _ in range(outputListLength)]


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
