from typing import List

import pandas as pd

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import emptyDataFrame, getAggregatedColumn


class CountFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int
    ) -> List[float]:
        return list(range(len(dataFrame) + 1))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        return len(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def getAggregationPacking(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: float,
            upperBound: float,
            possibleAggregations: List[float],
    ) -> pd.DataFrame:
        emptyFrame = emptyDataFrame(dataFrame.columns)
        aggregatedColumn = getAggregatedColumn(dataFrame, aggregationAttributeIndex)
        aggregatedColumnSize = len(aggregatedColumn)

        if aggregatedColumnSize < lowerBound:
            return emptyFrame

        amountTuplesToReturn = min(upperBound, aggregatedColumnSize)

        return dataFrame.iloc[:amountTuplesToReturn]

    def __str__(self):
        return "COUNT"
