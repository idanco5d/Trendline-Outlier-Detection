from typing import Set, Dict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction, emptyDataFrame


class CountDistinctFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> Set[int]:
        return set(range(len(dataFrame.iloc[:, aggregationAttributeIndex].unique()) + 1))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> int:
        aggregationColumn = dataFrame.iloc[:, aggregationAttributeIndex]
        return aggregationColumn.nunique()

    def getBoundedAggregation(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: int,
            upperBound: int
    ) -> pd.DataFrame:
        result = dataFrame.copy()

        while True:
            aggregatedColumn = result.iloc[:, aggregationAttributeIndex]
            valuesCount: Dict[int, int] = aggregatedColumn.value_counts().to_dict()
            countDistinct: int = aggregatedColumn.nunique()

            if countDistinct <= upperBound:
                break

            values = valuesCount.values()
            if len(values) == 0:
                break

            minCount = min(values)
            leastCommonValues = [val for val, count in valuesCount.items() if count == minCount]

            result = result[~aggregatedColumn.isin(leastCommonValues)]

        if countDistinct < lowerBound:
            return emptyDataFrame(dataFrame.columns)

        return result

    def __str__(self):
        return "COUNT_DISTINCT"
