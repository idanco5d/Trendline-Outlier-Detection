from collections import defaultdict
from typing import Set, DefaultDict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import emptyDataFrame, getAggregatedColumn


class MinFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> DefaultDict[int, Set[float]]:
        return defaultdict(lambda: set(getAggregatedColumn(dataFrame, aggregationAttributeIndex)))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        return min(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def getAggregationPacking(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: float,
            upperBound: float,
            possibleAggregations: Set[float],
    ) -> pd.DataFrame:
        emptyFrame = emptyDataFrame(dataFrame.columns)
        result = emptyFrame.copy()
        minValue = float('inf')

        for index, datasetTuple in dataFrame.iterrows():
            currentValue = datasetTuple.iloc[aggregationAttributeIndex]
            if currentValue >= lowerBound:
                result.loc[index] = datasetTuple
            if currentValue < minValue:
                minValue = currentValue

        if minValue > upperBound:
            return emptyFrame
        return result

    def __str__(self):
        return "MIN"
