from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction


class SumFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> Set[int]:
        pass

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> int:
        aggregationColumn = dataFrame.iloc[:, aggregationAttributeIndex]
        return sum(aggregationColumn)

    def getBoundedAggregation(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: int,
            upperBound: int
    ) -> pd.DataFrame:
        pass

    def __str__(self):
        return "SUM"
