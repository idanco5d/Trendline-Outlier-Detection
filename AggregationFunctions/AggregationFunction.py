from abc import ABC, abstractmethod
from typing import Set, DefaultDict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


class AggregationFunction(ABC):

    @abstractmethod
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> DefaultDict[int, Set[float]]:
        pass

    @abstractmethod
    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> float:
        pass

    @abstractmethod
    def getAggregationPacking(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: float,
            upperBound: float,
            possibleAggregations: Set[float],
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def __str__(self):
        pass
