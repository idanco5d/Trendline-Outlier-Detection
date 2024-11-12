from typing import Set, List, Dict, Hashable

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import listOfEmptyDictionaries, getGroupByKey, emptyDataFrame, dataFramesUnion


# TODO fix cases with several rows of grouped


def calculateOptimalSubsetWithConstraint(
        groupedRows: DataFrameGroupBy,
        aggregationFunction: AggregationFunction,
        possibleAggregations: Set[int],
        aggregationAttributeIndex: int
) -> pd.DataFrame:
    groupingValues = iter(groupedRows.groups.keys())
    minimalGroupingValueGroup = getGroupByKey(groupedRows, next(groupingValues))
    solutions: List[Dict[int, pd.DataFrame]] = listOfEmptyDictionaries(len(groupedRows.groups.keys()))

    calculateMinimalValueGroupSolution(
        possibleAggregations,
        solutions,
        aggregationFunction,
        minimalGroupingValueGroup,
        aggregationAttributeIndex
    )

    sortedPossibleAggregations = sorted(possibleAggregations)
    possibleAggregationsLength = len(sortedPossibleAggregations)

    for currentIndex, groupingValue in enumerate(groupingValues, start=1):
        solutionMaxSizePerUpperBound: List[int] = [0 for _ in range(possibleAggregationsLength)]

        for i in range(possibleAggregationsLength):
            for j in range(i, possibleAggregationsLength):
                lowerBound = sortedPossibleAggregations[i]
                upperBound = sortedPossibleAggregations[j]

                currentBoundsSolution = calculateCurrentBoundsSolution(aggregationFunction, groupedRows, groupingValue,
                                                                       aggregationAttributeIndex, lowerBound, upperBound,
                                                                       solutions, currentIndex)
                currentBoundsSolutionLength = len(currentBoundsSolution)

                if currentBoundsSolutionLength > solutionMaxSizePerUpperBound[j]:
                    solutionMaxSizePerUpperBound[j] = currentBoundsSolutionLength
                    solutions[currentIndex][upperBound] = currentBoundsSolution

    if len(solutions[-1].values()) == 0:
        return emptyDataFrame(minimalGroupingValueGroup.columns)
    return max(solutions[-1].values(), key=lambda df: df.size)


def calculateMinimalValueGroupSolution(
        possibleAggregations: Set[int],
        possibleSolutions: List[Dict[int, pd.DataFrame]],
        aggregationFunction: AggregationFunction,
        minimalValueGroup: pd.DataFrame,
        aggregationAttributeIndex: int
):
    for upperBound in possibleAggregations:
        possibleSolutions[0][upperBound] = aggregationFunction.getAggregationPacking(
            minimalValueGroup,
            aggregationAttributeIndex,
            min(possibleAggregations),
            upperBound
        )


def calculateCurrentBoundsSolution(
        aggregationFunction: AggregationFunction,
        groupedRows: DataFrameGroupBy,
        groupingValue: Hashable,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int,
        possibleSolutions: List[Dict[int, pd.DataFrame]],
        currentIndex: int
):
    aggregationPacking = aggregationFunction.getAggregationPacking(
        getGroupByKey(groupedRows, groupingValue),
        aggregationAttributeIndex,
        lowerBound,
        upperBound
    )

    if len(aggregationPacking) == 0:
        intermediateSolution = possibleSolutions[currentIndex - 1][lowerBound]
    else:
        aggregation = aggregationFunction.aggregate(aggregationPacking, aggregationAttributeIndex)
        intermediateSolution = dataFramesUnion(
            possibleSolutions[currentIndex - 1][aggregation], aggregationPacking
        )

    return intermediateSolution
