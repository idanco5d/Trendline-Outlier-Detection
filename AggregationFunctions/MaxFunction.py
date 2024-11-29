from collections import defaultdict
from typing import Set, DefaultDict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import emptyDataFrame, getAggregatedColumn


class MaxFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> DefaultDict[int, Set[float]]:
        return defaultdict(lambda: set(getAggregatedColumn(dataFrame, aggregationAttributeIndex)))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        return max(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

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
        maxValue = float('-inf')

        for index, datasetTuple in dataFrame.iterrows():
            currentValue = datasetTuple.iloc[aggregationAttributeIndex]
            if currentValue <= upperBound:
                result.loc[index] = datasetTuple
            if currentValue > maxValue:
                maxValue = currentValue

        if maxValue < lowerBound:
            return emptyFrame
        return result

    def __str__(self):
        return "MAX"
