import math
from collections import defaultdict
from typing import Tuple, DefaultDict, List

import pandas as pd

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import getAggregatedColumn, emptyDataFrame, dataFramesUnion


class SumFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int
    ) -> List[float]:
        aggregatedColumn = getAggregatedColumn(dataFrame, aggregationAttributeIndex)
        return list(range(math.ceil(max(aggregatedColumn)) * len(aggregatedColumn) + 1))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        return sum(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def getAggregationPacking(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: float,
            upperBound: float,
            possibleAggregations: List[float],
    ) -> pd.DataFrame:
        subsetsSizes: DefaultDict[Tuple[int, float], float] = defaultdict(lambda: float('-inf'))
        aggregationPackings: DefaultDict[Tuple[int, float], pd.DataFrame] = defaultdict(
            lambda: emptyDataFrame(dataFrame.columns)
        )

        setFirstAggregationPackingAndSubsetsSizes(
            aggregationAttributeIndex,
            aggregationPackings,
            dataFrame,
            possibleAggregations,
            subsetsSizes
        )

        for possibleAggregation in possibleAggregations:
            for j in range(1, len(dataFrame)):
                setCurrentAggregationPackingAndSubsetsSizes(
                    aggregationAttributeIndex,
                    aggregationPackings,
                    dataFrame,
                    j,
                    possibleAggregation,
                    subsetsSizes
                )

        return calculateOptimalPacking(
            aggregationPackings,
            dataFrame,
            lowerBound,
            possibleAggregations,
            subsetsSizes,
            upperBound
        )

    def __str__(self):
        return "SUM"


def setFirstAggregationPackingAndSubsetsSizes(
        aggregationAttributeIndex: int,
        aggregationPackings: DefaultDict[Tuple[int, float], pd.DataFrame],
        dataFrame: pd.DataFrame,
        possibleAggregations: List[float],
        subsetsSizes: DefaultDict[Tuple[int, float], float]
):
    firstRow = dataFrame.iloc[[0]]
    for possibleAggregation in possibleAggregations:
        if firstRow.iloc[0, aggregationAttributeIndex] == possibleAggregation:
            subsetsSizes[(0, possibleAggregation)] = 1
            aggregationPackings[(0, possibleAggregation)] = firstRow


def setCurrentAggregationPackingAndSubsetsSizes(
        aggregationAttributeIndex: int,
        aggregationPackings: DefaultDict[Tuple[int, float], pd.DataFrame],
        dataFrame: pd.DataFrame,
        j: int,
        possibleAggregation: float,
        subsetsSizes: DefaultDict[Tuple[int, float], float]
):
    currentValue = dataFrame.iloc[j, aggregationAttributeIndex]
    addIndicatorTuple = (j - 1, possibleAggregation - currentValue)
    skipIndicatorTuple = (j - 1, possibleAggregation)
    currentIterationTuple = (j, possibleAggregation)

    addCurrentRowIndicator = (
            subsetsSizes[addIndicatorTuple] + 1
    )
    skipCurrentRowIndicator = subsetsSizes[skipIndicatorTuple]

    if addCurrentRowIndicator > skipCurrentRowIndicator:
        subsetsSizes[currentIterationTuple] = addCurrentRowIndicator
        aggregationPackings[currentIterationTuple] = dataFramesUnion(
            aggregationPackings[addIndicatorTuple], dataFrame.iloc[[j]]
        )
    else:
        subsetsSizes[currentIterationTuple] = skipCurrentRowIndicator
        aggregationPackings[currentIterationTuple] = aggregationPackings[skipIndicatorTuple]


def calculateOptimalPacking(
        aggregationPackings: DefaultDict[Tuple[int, float], pd.DataFrame],
        dataFrame: pd.DataFrame,
        lowerBound: float,
        possibleAggregations: List[float],
        subsetsSizes: DefaultDict[Tuple[int, float], float],
        upperBound: float
):
    maxAggregation = float('-inf')
    result: pd.DataFrame = emptyDataFrame(dataFrame.columns)

    for possibleAggregation in possibleAggregations:
        if lowerBound <= possibleAggregation <= upperBound:
            currentSubsetTuple = (len(dataFrame) - 1, possibleAggregation)
            currentSubsetSize = subsetsSizes[currentSubsetTuple]

            if currentSubsetSize > maxAggregation:
                maxAggregation = currentSubsetSize
                result = aggregationPackings[currentSubsetTuple]

    return result
