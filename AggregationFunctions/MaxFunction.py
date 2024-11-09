from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import emptyDataFrame, getAggregatedColumn


class MaxFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> Set[int]:
        return set(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> int:
        return max(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def getBoundedAggregation(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: int,
            upperBound: int
    ) -> pd.DataFrame:
        emptyFrame = emptyDataFrame(dataFrame.columns)

        result = emptyFrame.copy()
        maxValue: int = -2 ** 31

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
