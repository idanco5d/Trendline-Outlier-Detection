from typing import Set, List, Dict, Tuple, Hashable

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import listOfEmptyDictionaries


def calculateOptimalSubsetWithConstraint(
        groupedRows: DataFrameGroupBy,
        aggregationFunction: AggregationFunction,
        possibleAggregations: Set[int],
        aggregationAttributeIndex: int
) -> pd.DataFrame:
    groupingValues = iter(groupedRows.groups.keys())
    minimalValueGroup = groupedRows.get_group(next(groupingValues))
    possibleSolutions: List[Dict[int, pd.DataFrame]] = listOfEmptyDictionaries(len(groupedRows.groups.keys()))

    calculateMinimalValueGroupSolution(
        possibleAggregations,
        possibleSolutions,
        aggregationFunction,
        minimalValueGroup,
        aggregationAttributeIndex
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

                intermediateSolution = calculateIntermediateSolution(aggregationFunction, groupedRows, groupingValue,
                                                                     aggregationAttributeIndex, lowerBound, upperBound,
                                                                     possibleSolutions, currentIndex)

                intermediateSolutions[(lowerBound, upperBound)] = intermediateSolution

                if len(intermediateSolution) > intermediateSolutionMaxSize:
                    intermediateSolutionMaxSize = len(intermediateSolution)
                    possibleSolutions[currentIndex][upperBound] = intermediateSolution

        return max(possibleSolutions[-1].values(), key=lambda df: df.size)


def calculateMinimalValueGroupSolution(
        possibleAggregations: Set[int],
        possibleSolutions: List[Dict[int, pd.DataFrame]],
        aggregationFunction: AggregationFunction,
        minimalValueGroup: pd.DataFrame,
        aggregationAttributeIndex: int
):
    for upperBound in possibleAggregations:
        possibleSolutions[0][upperBound] = aggregationFunction.getBoundedAggregation(
            minimalValueGroup,
            aggregationAttributeIndex,
            min(possibleAggregations),
            upperBound
        )


def calculateIntermediateSolution(
        aggregationFunction: AggregationFunction,
        groupedRows: DataFrameGroupBy,
        groupingValue: Hashable,
        aggregationAttributeIndex: int,
        lowerBound: int,
        upperBound: int,
        possibleSolutions: List[Dict[int, pd.DataFrame]],
        currentIndex: int
):
    boundedAggregation = aggregationFunction.getBoundedAggregation(
        groupedRows.get_group(groupingValue),
        aggregationAttributeIndex,
        lowerBound,
        upperBound
    )

    if len(boundedAggregation) == 0:
        intermediateSolution = possibleSolutions[currentIndex - 1][lowerBound]
    else:
        aggregation = aggregationFunction.aggregate(boundedAggregation, aggregationAttributeIndex)
        intermediateSolution = dataFramesUnion(
            possibleSolutions[currentIndex - 1][aggregation], boundedAggregation
        )

    return intermediateSolution


def dataFramesUnion(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    combined_df = pd.concat([df1, df2])
    result_df = combined_df[~combined_df.index.duplicated(keep='first')]

    return result_df.sort_index()
