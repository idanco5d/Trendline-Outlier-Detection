from typing import List

import pandas as pd

from aggregations.aggregation import Aggregation


class Sum(Aggregation):

    def get_possible_subsets_aggregations(self, df: pd.DataFrame, agg_col: str) -> List[float]:
        return list(range(df[agg_col].sum() + 1))

    def aggregate(self, df: pd.DataFrame, agg_col: str) -> float:
        return df[agg_col].sum()

    def get_aggregation_packing(self, df: pd.DataFrame, agg_col: str, low: float, high: float) -> pd.DataFrame:
        values = df[agg_col].tolist()

        # Dynamic programming dictionary: key = sum, value = (maximal_subset_size, subset_indices)
        largest_subsets = {0: (0, [])}  # Initialize with sum 0 having 0 rows and an empty subset

        for i, value in enumerate(values):
            current_subsets = largest_subsets.copy()  # Avoid modifying dict while iterating

            for current_sum, (row_count, indices) in largest_subsets.items():
                new_sum = current_sum + value
                if new_sum > high:
                    continue
                if new_sum not in current_subsets or current_subsets[new_sum][0] < row_count + 1:
                    current_subsets[new_sum] = (row_count + 1, indices + [i])

            largest_subsets = current_subsets

        best_subset = None
        max_row_count = 0
        for possible_sum, (row_count, indices) in largest_subsets.items():
            if low <= possible_sum <= high and row_count > max_row_count:
                max_row_count = row_count
                best_subset = indices

        if best_subset is not None:
            return df.iloc[best_subset]
        return pd.DataFrame(columns=df.columns)

    def __str__(self):
        return "SUM"
