from collections import defaultdict
from typing import Set, Dict, Tuple, DefaultDict, List

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from AggregationFunctions.CountFunction import CountFunction
from AggregationFunctions.SumFunction import SumFunction
from Utils import getAggregatedColumn, emptyDataFrame, dataFramesUnion


class AverageFunction(AggregationFunction):
    def __init__(self):
        super().__init__()
        self.sumPossibleAggregations: List[float] = []
        self.countPossibleAggregations: List[float] = []

    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int
    ) -> List[float]:

        self.sumPossibleAggregations = SumFunction().getPossibleSubsetsAggregations(
            dataFrame, aggregationAttributeIndex
        )
        self.countPossibleAggregations = CountFunction().getPossibleSubsetsAggregations(
            dataFrame, aggregationAttributeIndex
        )

        avgPossibleAggregations = set()

        for x in self.sumPossibleAggregations:
            for k in self.countPossibleAggregations:
                if k != 0:
                    avgPossibleAggregations.add(x / k)

        return sorted(avgPossibleAggregations)

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        aggregationColumn = getAggregatedColumn(dataFrame, aggregationAttributeIndex)
        return sum(aggregationColumn) / len(aggregationColumn)

    def getAggregationPacking(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: float,
            upperBound: float,
            possibleAggregations: List[float],
    ) -> pd.DataFrame:
        subsetsExistenceWithSize: DefaultDict[Tuple[int, float, float], float] = defaultdict(lambda: float('-inf'))
        aggregationPackings: DefaultDict[Tuple[int, float, float], pd.DataFrame] = defaultdict(
            lambda: emptyDataFrame(dataFrame.columns)
        )

        self.setFirstAggregationPackingAndSubsetsExistence(
            aggregationAttributeIndex,
            aggregationPackings,
            dataFrame,
            subsetsExistenceWithSize
        )

        for sumAggregation in self.sumPossibleAggregations:
            for countAggregation in self.countPossibleAggregations:
                if countAggregation == 0:
                    continue
                for j in range(1, len(dataFrame)):
                    setCurrentAggregationPackingsAndSubsetsExistence(
                        aggregationAttributeIndex,
                        aggregationPackings,
                        countAggregation,
                        dataFrame,
                        j,
                        subsetsExistenceWithSize,
                        sumAggregation
                    )

        return self.calculateOptimalPacking(
            aggregationPackings,
            dataFrame,
            lowerBound,
            subsetsExistenceWithSize,
            upperBound
        )

    def setFirstAggregationPackingAndSubsetsExistence(
            self,
            aggregationAttributeIndex: int,
            aggregationPackings: DefaultDict[Tuple[int, float, float], pd.DataFrame],
            dataFrame: pd.DataFrame,
            subsetsExistenceWithSize: DefaultDict[Tuple[int, float, float], float]
    ):
        firstRow = dataFrame.iloc[[0]]

        for sumAggregation in self.sumPossibleAggregations:
            if firstRow.iloc[0, aggregationAttributeIndex] == sumAggregation:
                subsetsExistenceWithSize[(0, sumAggregation, 0)] = 1
                aggregationPackings[(0, sumAggregation, 0)] = firstRow

    def calculateOptimalPacking(
            self,
            aggregationPackings: DefaultDict[Tuple[int, float, float], pd.DataFrame],
            dataFrame: pd.DataFrame,
            lowerBound: float,
            subsetsExistenceWithSize: DefaultDict[Tuple[int, float, float], float],
            upperBound: float
    ):
        maxAggregation = float('-inf')
        result: pd.DataFrame = emptyDataFrame(dataFrame.columns)

        for sumAggregation in self.sumPossibleAggregations:
            for countAggregation in self.countPossibleAggregations:
                if countAggregation == 0:
                    continue
                if lowerBound <= sumAggregation / countAggregation <= upperBound:
                    currentSubsetTuple = (len(dataFrame) - 1, sumAggregation, countAggregation)
                    currentSubsetSize = subsetsExistenceWithSize[currentSubsetTuple]

                    if currentSubsetSize > maxAggregation:
                        maxAggregation = currentSubsetSize
                        result = aggregationPackings[currentSubsetTuple]

        return result

    def __str__(self):
        return "AVG"


def setCurrentAggregationPackingsAndSubsetsExistence(
        aggregationAttributeIndex: int,
        aggregationPackings: DefaultDict[Tuple[int, float, float], pd.DataFrame],
        countAggregation: float,
        dataFrame: pd.DataFrame,
        j: int,
        subsetsExistenceWithSize: DefaultDict[Tuple[int, float, float], float],
        sumAggregation: float
):
    currentIterationTuple = (j, sumAggregation, countAggregation)
    skipIndicatorTuple = (j - 1, sumAggregation, countAggregation)

    if countAggregation > j:
        subsetsExistenceWithSize[currentIterationTuple] = float('-inf')
        aggregationPackings[currentIterationTuple] = aggregationPackings[skipIndicatorTuple]
    else:
        currentValue = dataFrame.iloc[j, aggregationAttributeIndex]
        addIndicatorTuple = (j - 1, sumAggregation - currentValue, countAggregation - 1)

        addCurrentRowIndicator = (
                subsetsExistenceWithSize[addIndicatorTuple] + 1
        )
        skipCurrentRowIndicator = (
            subsetsExistenceWithSize[skipIndicatorTuple]
        )

        if addCurrentRowIndicator > skipCurrentRowIndicator:
            subsetsExistenceWithSize[currentIterationTuple] = addCurrentRowIndicator
            aggregationPackings[currentIterationTuple] = dataFramesUnion(
                aggregationPackings[addIndicatorTuple],
                dataFrame.iloc[[j]]
            )
        else:
            subsetsExistenceWithSize[currentIterationTuple] = skipCurrentRowIndicator
            aggregationPackings[currentIterationTuple] = aggregationPackings[
                skipIndicatorTuple
            ]

