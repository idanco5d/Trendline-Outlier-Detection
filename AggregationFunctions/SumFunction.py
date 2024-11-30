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

        firstTuple = dataFrame.iloc[[0]]
        for possibleAggregation in possibleAggregations:
            if firstTuple.iloc[0, aggregationAttributeIndex] == possibleAggregation:
                subsetsSizes[(0, possibleAggregation)] = 1
                aggregationPackings[(0, possibleAggregation)] = firstTuple

        for possibleAggregation in possibleAggregations:
            for j in range(1, len(dataFrame)):
                currentValue = dataFrame.iloc[j, aggregationAttributeIndex]
                addCurrentTupleIndicator = (
                        subsetsSizes[(j - 1, possibleAggregation - currentValue)] + 1
                )
                skipCurrentTupleIndicator = subsetsSizes[(j - 1, possibleAggregation)]
                if addCurrentTupleIndicator > skipCurrentTupleIndicator:
                    subsetsSizes[(j, possibleAggregation)] = addCurrentTupleIndicator
                    aggregationPackings[(j, possibleAggregation)] = dataFramesUnion(
                        aggregationPackings[(j - 1, possibleAggregation - currentValue)], dataFrame.iloc[[j]]
                    )
                else:
                    subsetsSizes[(j, possibleAggregation)] = skipCurrentTupleIndicator
                    aggregationPackings[(j, possibleAggregation)] = aggregationPackings[(j - 1, possibleAggregation)]

        maxAggregation = float('-inf')
        result: pd.DataFrame = emptyDataFrame(dataFrame.columns)
        for possibleAggregation in possibleAggregations:

            if lowerBound <= possibleAggregation <= upperBound:
                currentSubsetSize = subsetsSizes[(len(dataFrame) - 1, possibleAggregation)]
                if currentSubsetSize > maxAggregation:
                    maxAggregation = currentSubsetSize
                    result = aggregationPackings[(len(dataFrame) - 1, possibleAggregation)]

        return result

    def __str__(self):
        return "SUM"
