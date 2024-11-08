from typing import Dict

import pandas as pd

from AllowedAggregationFunction import AllowedAggregationFunction


# TODO remove file after creating the abstract class


def getBoundedAggregation(
        aggregationFunction: AllowedAggregationFunction,
        dataFrame: pd.DataFrame,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    match aggregationFunction:
        case AllowedAggregationFunction.MAX:
            return maxBoundedAggregation(dataFrame, aggregationAttributeIndex, lowerBound, upperBound)
        case AllowedAggregationFunction.MIN:
            return minBoundedAggregation(dataFrame, aggregationAttributeIndex, lowerBound, upperBound)
        case AllowedAggregationFunction.COUNT:
            return countBoundedAggregation(dataFrame, aggregationAttributeIndex, lowerBound, upperBound)
        case AllowedAggregationFunction.COUNT_DISTINCT:
            return countDistinctBoundedAggregation(dataFrame, aggregationAttributeIndex, lowerBound, upperBound)


def maxBoundedAggregation(
        dataFrame: pd.DataFrame,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    emptyFrame = emptyDataFrame(dataFrame.columns)

    result = emptyFrame.copy()
    maxValue: int = -2 ** 31

    for index, datasetTuple in dataFrame.iterrows():
        currentValue = datasetTuple.iloc[aggregationAttributeIndex]
        if currentValue <= upperBound:
            result.loc[index] = datasetTuple
        if currentValue > maxValue:
            maxValue = currentValue

    if maxValue < lowerBound:
        return emptyFrame
    return result


def minBoundedAggregation(
        dataFrame: pd.DataFrame,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    emptyFrame = emptyDataFrame(dataFrame.columns)
    result = emptyFrame.copy()
    minValue: int = 2 ** 31

    for index, datasetTuple in dataFrame.iterrows():
        currentValue = datasetTuple.iloc[aggregationAttributeIndex]
        if currentValue >= lowerBound:
            result.loc[index] = datasetTuple
        if currentValue < minValue:
            minValue = currentValue

    if minValue > upperBound:
        return emptyFrame
    return result


def countBoundedAggregation(
        dataFrame: pd.DataFrame,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    emptyFrame = emptyDataFrame(dataFrame.columns)
    aggregatedColumn = dataFrame.iloc[:, aggregationAttributeIndex]
    aggregatedColumnSize = len(aggregatedColumn)

    if aggregatedColumnSize < lowerBound:
        return emptyFrame

    amountTuplesToReturn = min(upperBound, aggregatedColumnSize)

    return dataFrame.iloc[:amountTuplesToReturn]


def countDistinctBoundedAggregation(
        dataFrame: pd.DataFrame,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    result = dataFrame.copy()

    while True:
        aggregatedColumn = result.iloc[:, aggregationAttributeIndex]
        valuesCount: Dict[int, int] = aggregatedColumn.value_counts().to_dict()
        countDistinct: int = aggregatedColumn.nunique()

        if countDistinct <= upperBound:
            break

        values = valuesCount.values()
        if len(values) == 0:
            break

        minCount = min(values)
        leastCommonValues = [val for val, count in valuesCount.items() if count == minCount]

        result = result[~aggregatedColumn.isin(leastCommonValues)]

    if countDistinct < lowerBound:
        return emptyDataFrame(dataFrame.columns)

    return result


def emptyDataFrame(baseDfColumns) -> pd.DataFrame:
    return pd.DataFrame(columns=baseDfColumns)
