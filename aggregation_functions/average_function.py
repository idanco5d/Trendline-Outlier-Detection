from collections import defaultdict
from typing import Tuple, DefaultDict, List

import pandas as pd

from Utils import get_aggregated_column, empty_data_frame
from aggregation_functions.aggregation_function import AggregationFunction
from aggregation_functions.count_function import CountFunction
from aggregation_functions.sum_function import SumFunction


class AverageFunction(AggregationFunction):
    def __init__(self):
        super().__init__()
        self.sum_possible_aggregations: List[float] = []
        self.count_possible_aggregations: List[float] = []
        self.subsets_existence_with_size: DefaultDict[Tuple[int, float, float], float | None] = defaultdict(
            lambda: None)
        self.aggregation_packings: DefaultDict[Tuple[int, float, float], pd.DataFrame | None] = defaultdict(
            lambda: None)

    def get_possible_subsets_aggregations(
            self, data_frame: pd.DataFrame, aggregation_attribute_index: int
    ) -> List[float]:

        self.sum_possible_aggregations = SumFunction().get_possible_subsets_aggregations(
            data_frame, aggregation_attribute_index
        )
        self.count_possible_aggregations = CountFunction().get_possible_subsets_aggregations(
            data_frame, aggregation_attribute_index
        )
        self.subsets_existence_with_size = defaultdict(lambda: None)
        self.aggregation_packings = defaultdict(lambda: None)

        avg_possible_aggregations = {x / k for x in self.sum_possible_aggregations
                                     for k in self.count_possible_aggregations if k != 0}

        return sorted(avg_possible_aggregations)

    def aggregate(self, data_frame: pd.DataFrame, aggregation_attribute_index: int) -> float:
        aggregation_column = get_aggregated_column(data_frame, aggregation_attribute_index)
        return aggregation_column.sum() / len(aggregation_column)

    def get_aggregation_packing(
            self,
            data_frame: pd.DataFrame,
            aggregation_attribute_index: int,
            lower_bound: float,
            upper_bound: float,
            possible_aggregations: List[float],
    ) -> pd.DataFrame:
        agg_col = data_frame.columns[aggregation_attribute_index]
        values = data_frame[agg_col].tolist()

        # DP table: sum_subsets[s][k] is a subset with sum s and size k, if such subset exists
        sum_subsets = {0: {0: []}}

        for i, value in enumerate(values):
            # keep a static copy of the keys because we are adding keys in the loop
            for current_sum in list(sum_subsets.keys()):
                for size in list(sum_subsets[current_sum].keys()):
                    subset = sum_subsets[current_sum][size]
                    if i in subset:
                        continue
                    new_sum = current_sum + value
                    new_size = size + 1
                    if new_sum not in sum_subsets:
                        sum_subsets[new_sum] = {}
                    if new_size not in sum_subsets[new_sum]:
                        sum_subsets[new_sum][new_size] = subset + [i]

        best_subset = None
        max_size = 0
        for possible_sum in sum_subsets:
            for size, subset in sum_subsets[possible_sum].items():
                if size > 0 and lower_bound <= possible_sum / size <= upper_bound and size > max_size:
                    best_subset = subset
                    max_size = size

        if best_subset is not None:
            return data_frame.iloc[best_subset]
        return empty_data_frame(data_frame.columns)

    def __str__(self):
        return "AVG"
