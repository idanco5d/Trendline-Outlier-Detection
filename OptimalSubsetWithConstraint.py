from typing import List, Dict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import listOfEmptyDictionaries, getGroupByKey, emptyDataFrame, dataFramesUnion


def calculateOptimalSubsetWithConstraint(
        groupedRows: DataFrameGroupBy,
        aggregationFunction: AggregationFunction,
        aggregationAttributeIndex: int
) -> pd.DataFrame:
    groupingValues = iter(groupedRows.groups.keys())
    minimalGroupingValueGroup = getGroupByKey(groupedRows, next(groupingValues))
    solutions = listOfEmptyDictionaries(
        len(groupedRows.groups.keys()), minimalGroupingValueGroup.columns
    )

    calculateMinimalValueGroupSolution(
        solutions,
        aggregationFunction,
        minimalGroupingValueGroup,
        aggregationAttributeIndex
    )

    for currentIndex, groupingValue in enumerate(groupingValues, start=1):
        currentValueGroup = getGroupByKey(groupedRows, groupingValue)
        currentPossibleAggregations = aggregationFunction.getPossibleSubsetsAggregations(
            currentValueGroup,
            aggregationAttributeIndex
        )
        possibleAggregationsLength = len(currentPossibleAggregations)
        solutionMaxSizePerUpperBound: List[int] = [0 for _ in range(possibleAggregationsLength)]

        for i in range(possibleAggregationsLength):
            lowerBound = currentPossibleAggregations[i]
            for j in range(i, possibleAggregationsLength):
                upperBound = currentPossibleAggregations[j]

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
        possibleSolutions: List[Dict[float, pd.DataFrame]],
        aggregationFunction: AggregationFunction,
        minimalValueGroup: pd.DataFrame,
        aggregationAttributeIndex: int
):
    possibleAggregations = aggregationFunction.getPossibleSubsetsAggregations(
        minimalValueGroup, aggregationAttributeIndex
    )
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
        possibleAggregations: List[float],
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
