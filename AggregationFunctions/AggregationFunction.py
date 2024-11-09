from abc import ABC, abstractmethod
from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


class AggregationFunction(ABC):

    @abstractmethod
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int, groupedRows: DataFrameGroupBy
    ) -> Set[int]:
        pass

    @abstractmethod
    def aggregate(self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int) -> int:
        pass

    @abstractmethod
    def getBoundedAggregation(
            self,
            dataFrame: pd.DataFrame,
            aggregationAttributeIndex: int,
            lowerBound: int,
            upperBound: int
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def __str__(self):
        pass


def emptyDataFrame(baseDfColumns) -> pd.DataFrame:
    return pd.DataFrame(columns=baseDfColumns)
