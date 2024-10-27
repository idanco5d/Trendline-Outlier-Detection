import pandas as pd

from AllowedAggregationFunction import AllowedAggregationFunction


def getBoundedAggregation(
        allowedAggregationFunction: AllowedAggregationFunction,
        dataFrame: pd.DataFrame,
        aggregationAttributeInd: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    match allowedAggregationFunction:
        case AllowedAggregationFunction.MAX:
            return maxBoundedAggregation(dataFrame, aggregationAttributeInd, lowerBound, upperBound)
        case AllowedAggregationFunction.MIN:
            return minBoundedAggregation(dataFrame, aggregationAttributeInd, lowerBound, upperBound)


def maxBoundedAggregation(
        dataFrame: pd.DataFrame,
        aggregationAttributeInd: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    emptyFrame = pd.DataFrame(columns=dataFrame.columns)
    result = emptyFrame
    maxValue: int = -2 ** 31

    for index, datasetTuple in dataFrame.iterrows():
        currentValue = datasetTuple.iloc[aggregationAttributeInd]
        if currentValue <= upperBound:
            result.loc[index] = datasetTuple
        if currentValue > maxValue:
            maxValue = currentValue

    if maxValue < lowerBound:
        return emptyFrame
    return result


def minBoundedAggregation(
        dataFrame: pd.DataFrame,
        aggregationAttributeInd: int,
        lowerBound: int,
        upperBound: int
) -> pd.DataFrame:
    emptyFrame: pd.DataFrame = pd.DataFrame(columns=dataFrame.columns)
    result = emptyFrame
    minValue: int = 2 ** 31

    for index, datasetTuple in dataFrame.iterrows():
        currentValue = datasetTuple.iloc[aggregationAttributeInd]
        if currentValue >= lowerBound:
            result.loc[index] = datasetTuple
        if currentValue < minValue:
            minValue = currentValue

    if minValue > upperBound:
        return emptyFrame
    return result


# def countDistinctBoundedAggregation(
#         dataFrame: pd.DataFrame,
#         aggregationAttributeInd: int,
#         lowerBound: int,
#         upperBound: int
# ) -> pd.DataFrame:
#     counts_dict = dataFrame.iloc[:, aggregationAttributeInd].value_counts().to_dict()
