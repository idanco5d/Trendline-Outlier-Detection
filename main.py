from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AllowedAggregationFunction import AllowedAggregationFunction
from InputParser import getParsedInput


def getPossibleSubsetsAggregations(
        functionType: AllowedAggregationFunction, dataFrame: pd.DataFrame, aggregationIndex: int
) -> Set[int]:
    match functionType:
        case AllowedAggregationFunction.MAX:
            return set(dataFrame.iloc[:, aggregationIndex])
        case AllowedAggregationFunction.MIN:
            return set(dataFrame.iloc[:, aggregationIndex])
        case AllowedAggregationFunction.COUNT_DISTINCT:
            return set(range(len(dataFrame.iloc[aggregationIndex])))


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


def dataFrameUnion(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    combined_df = pd.concat([df1, df2])
    result_df = combined_df[~combined_df.index.duplicated(keep='first')]

    return result_df.sort_index()


def optimalSolution(
        aggregationFunction: AllowedAggregationFunction,
        groupedRows: DataFrameGroupBy,
        aggregationAttributeInd: int,
        possibleAggregations: Set[int],
        boundary: int
) -> pd.DataFrame:
    firstGroup = groupedRows.get_group(
        next(iter(groupedRows.groups.keys()))
    )

    solution = getBoundedAggregation(
        aggregationFunction,
        firstGroup,
        aggregationAttributeInd,
        min(possibleAggregations),
        boundary
    )

    keysIterator = iter(groupedRows.groups.keys())
    next(keysIterator)

    for key in keysIterator:
        currGroup = groupedRows.get_group(key)
        currGroupSolution = solution

        for possibleAggregation in possibleAggregations:
            currBoundedAggregation = getBoundedAggregation(
                aggregationFunction, currGroup, aggregationAttributeInd, possibleAggregation, boundary
            )
            currSolution = dataFrameUnion(solution, currBoundedAggregation)

            if len(currSolution) > len(currGroupSolution):
                currGroupSolution = currSolution

        solution = currGroupSolution

    return solution


if __name__ == '__main__':
    aggrFunction, data, aggregationAttributeIndex, groupedRowsByValue = getParsedInput()

    possibleSubsetsAggregations = getPossibleSubsetsAggregations(
        aggrFunction, data, aggregationAttributeIndex
    )

    print("Data is: ", data)
    print("Aggregation function: ", aggrFunction)
    print("Possible subsets aggregations are: ", possibleSubsetsAggregations)
    print(
        "Optimal solution is: ",
        optimalSolution(
            aggrFunction,
            groupedRowsByValue,
            aggregationAttributeIndex,
            possibleSubsetsAggregations,
            max(possibleSubsetsAggregations)
        )
    )
