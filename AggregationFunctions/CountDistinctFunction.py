from typing import Dict, List

import pandas as pd

from AggregationFunctions.AggregationFunction import AggregationFunction
from Utils import emptyDataFrame, getAggregatedColumn


class CountDistinctFunction(AggregationFunction):
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int
    ) -> List[float]:
        return list(range(len(getAggregatedColumn(dataFrame, aggregationAttributeIndex).unique()) + 1))

    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        return getAggregatedColumn(dataFrame, aggregationAttributeIndex).nunique()

    def getAggregationPacking(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: float,
            upperBound: float,
            possibleAggregations: List[float],
    ) -> pd.DataFrame:
        result = dataFrame.copy()

        while True:
            aggregatedColumn = getAggregatedColumn(result, aggregationAttributeIndex)
            valuesCount: Dict[float, int] = aggregatedColumn.value_counts().to_dict()
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
