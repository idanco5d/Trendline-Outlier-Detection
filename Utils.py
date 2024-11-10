from typing import List, Dict, Hashable

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


def listOfEmptyDictionaries(outputListLength: int) -> List[Dict[int, pd.DataFrame]]:
    return [{} for _ in range(outputListLength)]


def calculateRemovedTuples(originalDataFrame: pd.DataFrame, containedDataFrame: pd.DataFrame) -> pd.DataFrame:
    mergedDataFrame = originalDataFrame.merge(containedDataFrame, how='outer', indicator=True)
    differenceDf = mergedDataFrame[mergedDataFrame['_merge'] == 'left_only']

    return differenceDf.drop(columns=['_merge'])


def emptyDataFrame(baseDfColumns) -> pd.DataFrame:
    return pd.DataFrame(columns=baseDfColumns)


def getAggregatedColumn(dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> pd.Series:
    return dataFrame.iloc[:, aggregationAttributeIndex]


def getGroupByKey(groupedRows: DataFrameGroupBy, key: Hashable) -> pd.DataFrame:
    firstKey = next(iter(groupedRows.groups))
    if isinstance(firstKey, tuple):
        return groupedRows.get_group(key)
    return groupedRows.get_group((key,))


def dataFramesUnion(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    combined_df = pd.concat([df1, df2])
    result_df = combined_df[~combined_df.index.duplicated(keep='first')]

    return result_df.sort_index()
