import pandas as pd

from AllowedAggregationFunction import AllowedAggregationFunction


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
        case AllowedAggregationFunction.COUNT_DISTINCT:
            return countDistinctBoundedAggregation(dataFrame, aggregationAttributeIndex, lowerBound, upperBound)


def maxBoundedAggregation(
        dataFrame: pd.DataFrame,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    emptyFrame = pd.DataFrame(columns=dataFrame.columns)

    result = emptyFrame
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
    emptyFrame: pd.DataFrame = pd.DataFrame(columns=dataFrame.columns)
    result = emptyFrame
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


def countDistinctBoundedAggregation(
        dataFrame: pd.DataFrame,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    result_df = dataFrame.copy()

    while True:
        counts_dict = result_df.iloc[:, aggregationAttributeIndex].value_counts().to_dict()

        if max(counts_dict.values()) <= upperBound:
            break

        min_count = min(counts_dict.values())
        least_common_values = [val for val, count in counts_dict.items()
                               if count == min_count]

        result_df = result_df[~result_df.iloc[:, aggregationAttributeIndex].isin(least_common_values)]

    if max(counts_dict.values()) < lowerBound:
        return pd.DataFrame(columns=dataFrame.columns)

    return result_df
