from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AllowedAggregationFunction import AllowedAggregationFunction
from BoundedAggregation import getBoundedAggregation
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


def dataFrameUnion(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    combined_df = pd.concat([df1, df2])
    result_df = combined_df[~combined_df.index.duplicated(keep='first')]

    return result_df.sort_index()


def optimalSolution(
        function: AllowedAggregationFunction,
        groupedRows: DataFrameGroupBy,
        aggregationAttributeInd: int,
        possibleAggregations: Set[int],
        boundary: int
) -> pd.DataFrame:
    firstGroup = groupedRows.get_group(
        next(iter(groupedRows.groups.keys()))
    )

    solution = getBoundedAggregation(
        function,
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
                function, currGroup, aggregationAttributeInd, possibleAggregation, boundary
            )
            currSolution = dataFrameUnion(solution, currBoundedAggregation)

            if len(currSolution) > len(currGroupSolution):
                currGroupSolution = currSolution

        solution = currGroupSolution

    return solution


if __name__ == '__main__':
    aggregationFunction, data, aggregationAttributeIndex, groupedRowsByValue = getParsedInput()

    possibleSubsetsAggregations = getPossibleSubsetsAggregations(
        aggregationFunction, data, aggregationAttributeIndex
    )

    print("Data is: ", data)
    print("Aggregation function: ", aggregationFunction)
    print("Possible subsets aggregations are: ", possibleSubsetsAggregations)
    print(
        "Optimal solution is: ",
        optimalSolution(
            aggregationFunction,
            groupedRowsByValue,
            aggregationAttributeIndex,
            possibleSubsetsAggregations,
            max(possibleSubsetsAggregations)
        )
    )
