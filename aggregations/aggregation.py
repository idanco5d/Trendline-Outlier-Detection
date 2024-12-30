from abc import ABC, abstractmethod
from typing import List

import pandas as pd


class Aggregation(ABC):

    # even though this returns a regular list and not a sorted one,
    # it should always be sorted since the groups are ordered in advanced in InputParser
    @abstractmethod
    def get_possible_subsets_aggregations(self, df: pd.DataFrame, agg_col: str) -> List[float]:
        pass

    @abstractmethod
    def aggregate(self, df: pd.DataFrame, agg_col: str) -> float:
        pass

    @abstractmethod
    def get_aggregation_packing(self, df: pd.DataFrame, agg_col: str, low: float, high: float) -> pd.DataFrame:
        pass

    @abstractmethod
    def __str__(self):
        pass
