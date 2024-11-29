from collections import defaultdict
from typing import Set, Dict, Tuple, DefaultDict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from AggregationFunctions.CountFunction import CountFunction
from AggregationFunctions.SumFunction import SumFunction
from Utils import getAggregatedColumn, emptyDataFrame, dataFramesUnion


class AverageFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> DefaultDict[int, Set[float]]:
        sumFunction = SumFunction()
        countFunction = CountFunction()

        sumPossibleAggregations = sumFunction.getPossibleSubsetsAggregations(
            dataFrame, aggregationAttributeIndex, groupedRows
        )
        countPossibleAggregations = countFunction.getPossibleSubsetsAggregations(
            dataFrame, aggregationAttributeIndex, groupedRows
        )

        avgPossibleAggregations: DefaultDict[int, Set[float]] = defaultdict(set)
        sumIndices = sumPossibleAggregations.keys()

        for index in sumIndices:
            for x in sumPossibleAggregations:
                for k in countPossibleAggregations:
                    avgPossibleAggregations[index].add(x / k)

        return avgPossibleAggregations

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        aggregationColumn = getAggregatedColumn(dataFrame, aggregationAttributeIndex)
        return sum(aggregationColumn) / len(aggregationColumn)

    def getAggregationPacking(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: float,
            upperBound: float,
            possibleAggregations: Set[float],
    ) -> pd.DataFrame:
        subsetsExistenceWithSize: DefaultDict[Tuple[int, float, int], float] = defaultdict(lambda: float('-inf'))
        aggregationPackings: Dict[float, pd.DataFrame] = {
            possibleAggregation: emptyDataFrame(dataFrame.columns)
            for possibleAggregation in possibleAggregations
        }

        firstTuple = dataFrame.iloc[[0]]
        for possibleAggregation in possibleAggregations:
            if firstTuple.iloc[0, aggregationAttributeIndex] == possibleAggregation:
                subsetsExistenceWithSize[(0, possibleAggregation, 0)] = 1
                aggregationPackings[possibleAggregation] = dataFramesUnion(
                    aggregationPackings[possibleAggregation], firstTuple
                )
            else:
                subsetsExistenceWithSize[(0, possibleAggregation, 0)] = float('-inf')

        for possibleAggregation in possibleAggregations:
            for k in range(len(possibleAggregations)):
                for j in range(1, len(dataFrame)):
                    if k > j:
                        subsetsExistenceWithSize[(j, possibleAggregation, k)] = float('-inf')
                    else:
                        addCurrentTupleIndicator = (
                                subsetsExistenceWithSize[
                                    (j - 1, possibleAggregation - dataFrame[j, aggregationAttributeIndex], k - 1)
                                ] + 1
                        )
                        skipCurrentTupleIndicator = (
                            subsetsExistenceWithSize[(j - 1, possibleAggregation, k)]
                        )
                        if addCurrentTupleIndicator > skipCurrentTupleIndicator:
                            subsetsExistenceWithSize[(k, possibleAggregation, j)] = addCurrentTupleIndicator
                            aggregationPackings[possibleAggregation] = dataFramesUnion(
                                aggregationPackings[possibleAggregation], dataFrame.iloc[[j]]
                            )
                        else:
                            subsetsExistenceWithSize[(k, possibleAggregation, j)] = skipCurrentTupleIndicator

        maxAggregation = float('-inf')
        result: pd.DataFrame = emptyDataFrame(dataFrame.columns)
        for possibleAggregation in possibleAggregations:
            for k in range(len(possibleAggregations)):
                if lowerBound <= possibleAggregation / k <= upperBound:
                    currentSubsetSize = subsetsExistenceWithSize[(len(dataFrame), possibleAggregation, k)]
                    if currentSubsetSize > maxAggregation:
                        maxAggregation = currentSubsetSize
                        result = aggregationPackings[possibleAggregation]

        return result

    def __str__(self):
        return "AVG"
