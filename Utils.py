from typing import List, Dict

import pandas as pd


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
