from abc import ABC, abstractmethod
from typing import List

import pandas as pd


class AggregationFunction(ABC):

    # even though this returns a regular list and not a sorted one,
    # it should always be sorted since the groups are ordered in advanced in InputParser
    @abstractmethod
    def get_possible_subsets_aggregations(
            self, data_frame: pd.DataFrame, aggregation_attribute_index: int
    ) -> List[float]:
        pass

    @abstractmethod
    def aggregate(self, data_frame: pd.DataFrame, aggregation_attribute_index: int) -> float:
        pass

    @abstractmethod
    def get_aggregation_packing(
            self,
            data_frame: pd.DataFrame,
            aggregation_attribute_index: int,
            lower_bound: float,
            upper_bound: float,
            possible_aggregations: List[float],
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def __str__(self):
        pass
