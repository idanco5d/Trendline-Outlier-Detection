from typing import Set, List, Dict, DefaultDict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import listOfEmptyDictionaries, getGroupByKey, emptyDataFrame, dataFramesUnion


def calculateOptimalSubsetWithConstraint(
        groupedRows: DataFrameGroupBy,
        aggregationFunction: AggregationFunction,
        possibleAggregations: DefaultDict[int, Set[float]],
        aggregationAttributeIndex: int
) -> pd.DataFrame:
    groupingValues = iter(groupedRows.groups.keys())
    minimalGroupingValueGroup = getGroupByKey(groupedRows, next(groupingValues))
    solutions = listOfEmptyDictionaries(
        len(groupedRows.groups.keys()), minimalGroupingValueGroup.columns
    )

    calculateMinimalValueGroupSolution(
        possibleAggregations[0],
        solutions,
        aggregationFunction,
        minimalGroupingValueGroup,
        aggregationAttributeIndex
    )

    for currentIndex, groupingValue in enumerate(groupingValues, start=1):
        currentValueGroup = getGroupByKey(groupedRows, groupingValue)
        currentPossibleAggregations = possibleAggregations[currentIndex]
        sortedPossibleAggregations = sorted(currentPossibleAggregations)
        possibleAggregationsLength = len(sortedPossibleAggregations)
        solutionMaxSizePerUpperBound: List[int] = [0 for _ in range(possibleAggregationsLength)]

        for i in range(possibleAggregationsLength):
            lowerBound = sortedPossibleAggregations[i]
            for j in range(i, possibleAggregationsLength):
                upperBound = sortedPossibleAggregations[j]

                currentBoundsSolution = calculateCurrentBoundsSolution(aggregationFunction, currentValueGroup,
                                                                       aggregationAttributeIndex, lowerBound,
                                                                       upperBound, currentPossibleAggregations,
                                                                       solutions, currentIndex)
                currentBoundsSolutionLength = len(currentBoundsSolution)

                if currentBoundsSolutionLength > solutionMaxSizePerUpperBound[j]:
                    solutionMaxSizePerUpperBound[j] = currentBoundsSolutionLength
                    solutions[currentIndex][upperBound] = currentBoundsSolution

    finalSolutionCandidates = solutions[-1].values()
    if len(finalSolutionCandidates) == 0:
        return emptyDataFrame(minimalGroupingValueGroup.columns)
    return max(finalSolutionCandidates, key=lambda df: df.size).sort_index()


def calculateMinimalValueGroupSolution(
        possibleAggregations: Set[float],
        possibleSolutions: List[Dict[float, pd.DataFrame]],
        aggregationFunction: AggregationFunction,
        minimalValueGroup: pd.DataFrame,
        aggregationAttributeIndex: int
):
    minPossibleAggregation = min(possibleAggregations)
    for upperBound in possibleAggregations:
        possibleSolutions[0][upperBound] = aggregationFunction.getAggregationPacking(
            minimalValueGroup,
            aggregationAttributeIndex,
            minPossibleAggregation,
            upperBound,
            possibleAggregations
        )


def calculateCurrentBoundsSolution(
        aggregationFunction: AggregationFunction,
        currentValueGroup: pd.DataFrame,
        aggregationAttributeIndex: int,
        lowerBound: float,
        upperBound: float,
        possibleAggregations: Set[float],
        possibleSolutions: List[Dict[float, pd.DataFrame]],
        currentIndex: int,
) -> pd.DataFrame:
    aggregationPacking = aggregationFunction.getAggregationPacking(
        currentValueGroup,
        aggregationAttributeIndex,
        lowerBound,
        upperBound,
        possibleAggregations
    )

    if len(aggregationPacking) == 0:
        currentBoundsSolution = possibleSolutions[currentIndex - 1][lowerBound]
    else:
        aggregation = aggregationFunction.aggregate(aggregationPacking, aggregationAttributeIndex)
        currentBoundsSolution = dataFramesUnion(
            possibleSolutions[currentIndex - 1][aggregation], aggregationPacking
        )

    return currentBoundsSolution
