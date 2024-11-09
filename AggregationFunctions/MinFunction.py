from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import emptyDataFrame, getAggregatedColumn


class MinFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> Set[int]:
        return set(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> int:
        return min(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def getBoundedAggregation(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: int,
            upperBound: int
    ) -> pd.DataFrame:
        emptyFrame = emptyDataFrame(dataFrame.columns)
        result = emptyFrame.copy()
        minValue: int = 2 ** 31

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
