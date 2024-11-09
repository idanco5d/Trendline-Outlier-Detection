from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import emptyDataFrame, getAggregatedColumn


class CountFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> Set[int]:
        groupsSizes = groupedRows.size()
        return set(range(groupsSizes.max() + 1))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> int:
        return len(getAggregatedColumn(dataFrame, aggregationAttributeIndex))

    def getBoundedAggregation(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: int,
            upperBound: int
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
