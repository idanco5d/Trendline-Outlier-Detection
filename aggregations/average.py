from collections import defaultdict
from typing import Tuple, DefaultDict, List

import pandas as pd

from aggregations.aggregation import Aggregation
from aggregations.count import Count
from aggregations.sum import Sum


class Average(Aggregation):
    def __init__(self):
        super().__init__()
        self.sum_possible_aggregations: List[float] = []
        self.count_possible_aggregations: List[float] = []
        self.subsets_existence_with_size: DefaultDict[Tuple[int, float, float], float | None] = defaultdict(
            lambda: None)
        self.aggregation_packings: DefaultDict[Tuple[int, float, float], pd.DataFrame | None] = defaultdict(
            lambda: None)

    def get_possible_subsets_aggregations(self, df: pd.DataFrame, agg_col: str) -> List[float]:
        self.sum_possible_aggregations = Sum().get_possible_subsets_aggregations(
            df, agg_col
        )
        self.count_possible_aggregations = Count().get_possible_subsets_aggregations(
            df, agg_col
        )
        self.subsets_existence_with_size = defaultdict(lambda: None)
        self.aggregation_packings = defaultdict(lambda: None)

        avg_possible_aggregations = {x / k for x in self.sum_possible_aggregations
                                     for k in self.count_possible_aggregations if k != 0}

        return sorted(avg_possible_aggregations)

    def aggregate(self, df: pd.DataFrame, agg_col: int) -> float:
        return df[agg_col].mean()

    def get_aggregation_packing(self, df: pd.DataFrame, agg_col: str, low: float, high: float) -> pd.DataFrame:
        values = df[agg_col].tolist()

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
                if size > 0 and low <= possible_sum / size <= high and size > max_size:
                    best_subset = subset
                    max_size = size

        if best_subset is not None:
            return df.iloc[best_subset]
        return pd.DataFrame(columns=df.columns)

    def __str__(self):
        return "AVG"
