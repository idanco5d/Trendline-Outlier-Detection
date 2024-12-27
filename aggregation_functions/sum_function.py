from collections import defaultdict
from typing import Tuple, DefaultDict, List

import pandas as pd

from utils import get_aggregated_column, empty_data_frame
from aggregation_functions.aggregation_function import AggregationFunction


class SumFunction(AggregationFunction):

    def __init__(self):
        super().__init__()
        self.subsets_sizes: DefaultDict[Tuple[int, float], float | None] = defaultdict(lambda: None)
        self.aggregation_packings: DefaultDict[Tuple[int, float], pd.DataFrame | None] = defaultdict(lambda: None)

    def get_possible_subsets_aggregations(
            self, data_frame: pd.DataFrame, aggregation_attribute_index: int
    ) -> List[float]:
        self.subsets_sizes = defaultdict(lambda: None)
        self.aggregation_packings = defaultdict(lambda: None)

        aggregated_column = get_aggregated_column(data_frame, aggregation_attribute_index)
        return list(range(aggregated_column.sum() + 1))

    def aggregate(self, data_frame: pd.DataFrame, aggregation_attribute_index: int) -> float:
        return get_aggregated_column(data_frame, aggregation_attribute_index).sum()

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

        # Dynamic programming dictionary: key = sum, value = (maximal_subset_size, subset_indices)
        largest_subsets = {0: (0, [])}  # Initialize with sum 0 having 0 rows and an empty subset

        for i, value in enumerate(values):
            current_subsets = largest_subsets.copy()  # Avoid modifying dict while iterating

            for current_sum, (row_count, indices) in largest_subsets.items():
                new_sum = current_sum + value
                if new_sum > upper_bound:
                    continue
                if new_sum not in current_subsets or current_subsets[new_sum][0] < row_count + 1:
                    current_subsets[new_sum] = (row_count + 1, indices + [i])

            largest_subsets = current_subsets

        best_subset = None
        max_row_count = 0
        for possible_sum, (row_count, indices) in largest_subsets.items():
            if lower_bound <= possible_sum <= upper_bound and row_count > max_row_count:
                max_row_count = row_count
                best_subset = indices

        if best_subset is not None:
            return data_frame.iloc[best_subset]
        return empty_data_frame(data_frame.columns)

    def __str__(self):
        return "SUM"


def get_aggregation_packing_old_logic(
        data_frame: pd.DataFrame,
        aggregation_attribute_index: int,
        lower_bound: float,
        upper_bound: float,
        possible_aggregations: List[float],
) -> pd.DataFrame:
    agg_col = data_frame.columns[aggregation_attribute_index]
    values = data_frame[agg_col].tolist()
    num_values = len(values)
    max_sum = sum(values)

    # Initialize the DP table largest_subsets[j][possible_sum]
    # largest_subsets[j][possible_sum] stores the largest subset of the first j rows with sum possible_sum
    largest_subsets = [[None for _ in range(max_sum + 1)] for _ in range(num_values + 1)]
    largest_subsets[0][0] = []  # Base case: with 0 rows, subset sum of 0 is an empty subset

    for j in range(1, num_values + 1):
        current_value = values[j - 1]
        for possible_sum in range(max_sum + 1):
            # Option 1: Exclude the current value
            if largest_subsets[j - 1][possible_sum] is not None:
                largest_subsets[j][possible_sum] = largest_subsets[j - 1][possible_sum]

            # Option 2: Include the current value (if valid)
            if possible_sum >= current_value and largest_subsets[j - 1][possible_sum - current_value] is not None:
                new_subset = largest_subsets[j - 1][possible_sum - current_value] + [j - 1]
                if (largest_subsets[j][possible_sum] is None) or (
                        len(new_subset) > len(largest_subsets[j][possible_sum])
                ):
                    largest_subsets[j][possible_sum] = new_subset  # Update with the larger subset

    # Find the best sum within [lower_bound, upper_bound]
    best_subset = None
    for possible_sum in range(int(lower_bound), int(upper_bound) + 1):
        if largest_subsets[num_values][possible_sum] is not None and (
                best_subset is None or len(largest_subsets[num_values][possible_sum]) > len(best_subset)
        ):
            best_subset = largest_subsets[num_values][possible_sum]

    if best_subset is not None:
        return data_frame.iloc[best_subset]
    return empty_data_frame(data_frame.columns)
