from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction, emptyDataFrame


class CountFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> Set[int]:
        groupsSizes = groupedRows.size()
        return set(range(groupsSizes.max() + 1))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> int:
        aggregationColumn = dataFrame.iloc[:, aggregationAttributeIndex]
        return len(aggregationColumn)

    def getBoundedAggregation(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: int,
            upperBound: int
    ) -> pd.DataFrame:
        emptyFrame = emptyDataFrame(dataFrame.columns)
        aggregatedColumn = dataFrame.iloc[:, aggregationAttributeIndex]
        aggregatedColumnSize = len(aggregatedColumn)

        if aggregatedColumnSize < lowerBound:
            return emptyFrame

        amountTuplesToReturn = min(upperBound, aggregatedColumnSize)

        return dataFrame.iloc[:amountTuplesToReturn]

    def __str__(self):
        return "COUNT"
