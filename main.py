from typing import Set, Dict, Tuple, List

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AllowedAggregationFunction import AllowedAggregationFunction, aggregate
from BoundedAggregation import getBoundedAggregation
from InputParser import parseInput


def getPossibleSubsetsAggregations(
        functionType: AllowedAggregationFunction,
        dataFrame: pd.DataFrame,
        aggregateIndex: int,
        groupedRows: DataFrameGroupBy
) -> Set[int]:
    match functionType:
        case AllowedAggregationFunction.MAX:
            return set(dataFrame.iloc[:, aggregateIndex])
        case AllowedAggregationFunction.MIN:
            return set(dataFrame.iloc[:, aggregateIndex])
        case AllowedAggregationFunction.COUNT:
            groupsSizes = groupedRows.size()
            return set(range(groupsSizes.max() + 1))
        case AllowedAggregationFunction.COUNT_DISTINCT:
            return set(range(len(dataFrame.iloc[:, aggregateIndex].unique()) + 1))
        case _:
            raise ValueError(f'Unsupported aggregation function: {functionType}')


def calculateOptimalSolution(
        aggregationFunction: AllowedAggregationFunction,
        groupedRows: DataFrameGroupBy,
        aggregationAttributeIndex: int,
        possibleAggregations: Set[int],
) -> pd.DataFrame:
    groupingValues = iter(groupedRows.groups.keys())
    minimalValueGroup = groupedRows.get_group(next(groupingValues))
    possibleSolutions: List[Dict[int, pd.DataFrame]] = listOfEmptyDictionaries(groupedRows)

    calculateMinimalValueGroupSolution(
        aggregationAttributeIndex,
        aggregationFunction,
        minimalValueGroup,
        possibleAggregations,
        possibleSolutions
    )

    sortedPossibleAggregations = sorted(possibleAggregations)
    possibleAggregationsLength = len(sortedPossibleAggregations)

    for currentIndex, groupingValue in enumerate(groupingValues, start=1):
        intermediateSolutions: Dict[Tuple[int, int], pd.DataFrame] = {}
        intermediateSolutionMaxSize = 0

        for i in range(possibleAggregationsLength):
            for j in range(i, possibleAggregationsLength):
                lowerBound = sortedPossibleAggregations[i]
                upperBound = sortedPossibleAggregations[j]

                intermediateSolution = calculateIntermediateSolution(
                    aggregationAttributeIndex,
                    aggregationFunction,
                    currentIndex,
                    groupedRows,
                    groupingValue,
                    lowerBound,
                    possibleSolutions,
                    upperBound
                )

                intermediateSolutions[(lowerBound, upperBound)] = intermediateSolution

                if len(intermediateSolution) > intermediateSolutionMaxSize:
                    intermediateSolutionMaxSize = len(intermediateSolution)
                    possibleSolutions[currentIndex][upperBound] = intermediateSolution

        return max(possibleSolutions[-1].values(), key=lambda df: df.size)


def listOfEmptyDictionaries(groupedRows):
    return [{} for _ in range(len(groupedRows.groups.keys()))]


def calculateMinimalValueGroupSolution(aggregationAttributeIndex, aggregationFunction, minimalValueGroup,
                                       possibleAggregations, possibleSolutions):
    for upperBound in possibleAggregations:
        possibleSolutions[0][upperBound] = getBoundedAggregation(
            aggregationFunction,
            minimalValueGroup,
            aggregationAttributeIndex,
            min(possibleAggregations),
            upperBound
        )


def calculateIntermediateSolution(aggregationAttributeIndex, aggregationFunction, currentIndex, groupedRows,
                                  groupingValue, lowerBound, possibleSolutions, upperBound):
    boundedAggregation = getBoundedAggregation(
        aggregationFunction,
        groupedRows.get_group(groupingValue),
        aggregationAttributeIndex,
        lowerBound,
        upperBound
    )

    if len(boundedAggregation) == 0:
        intermediateSolution = possibleSolutions[currentIndex - 1][lowerBound]
    else:
        aggregation = aggregate(aggregationFunction, boundedAggregation, aggregationAttributeIndex)
        intermediateSolution = dataFramesUnion(
            possibleSolutions[currentIndex - 1][aggregation], boundedAggregation
        )

    return intermediateSolution


def dataFramesUnion(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    combined_df = pd.concat([df1, df2])
    result_df = combined_df[~combined_df.index.duplicated(keep='first')]

    return result_df.sort_index()


if __name__ == '__main__':
    inputFunction, data, aggregationIndex, groupedRowsByValue = parseInput()

    possibleSubsetsAggregations = getPossibleSubsetsAggregations(
        inputFunction, data, aggregationIndex, groupedRowsByValue
    )

    print("Data is: \n", data)
    print("Aggregation function: ", inputFunction)
    print("Possible subsets aggregations are: ", possibleSubsetsAggregations)
    print(
        "Optimal solution is: \n",
        calculateOptimalSolution(
            inputFunction,
            groupedRowsByValue,
            aggregationIndex,
            possibleSubsetsAggregations,
        )
    )
