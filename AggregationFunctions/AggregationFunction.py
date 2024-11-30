from abc import ABC, abstractmethod
from typing import Set, List

import pandas as pd


class AggregationFunction(ABC):

    # even though this returns a regular list and not a sorted one,
    # it should always be sorted since the groups are ordered in advanced in InputParser
    @abstractmethod
    def getPossibleSubsetsAggregations(
            self, dataFrame: pd.DataFrame, aggregationAttributeIndex: int
    ) -> List[float]:
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
            possibleAggregations: List[float],
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def __str__(self):
        pass
