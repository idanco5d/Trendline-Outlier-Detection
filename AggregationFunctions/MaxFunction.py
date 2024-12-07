from typing import List

import pandas as pd

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import emptyDataFrame, getAggregatedColumn, getListOfColumnValues


class MaxFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int
    ) -> List[float]:
        return getListOfColumnValues(dataFrame, aggregationAttributeIndex)

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        return max(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def getAggregationPacking(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: float,
            upperBound: float,
            possibleAggregations: List[float],
    ) -> pd.DataFrame:
        result = emptyDataFrame(dataFrame.columns)
        maxValue = float('-inf')

        for index, datasetTuple in dataFrame.iterrows():
            currentValue = datasetTuple.iloc[aggregationAttributeIndex]
            if currentValue <= upperBound:
                result.loc[index] = datasetTuple
            if currentValue > maxValue:
                maxValue = currentValue

        if maxValue < lowerBound:
            return emptyDataFrame(dataFrame.columns)
        return result

    def __str__(self):
        return "MAX"
