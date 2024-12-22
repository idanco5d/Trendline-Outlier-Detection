import math
from collections import defaultdict
from typing import Tuple, DefaultDict, List

import pandas as pd

from aggregation_functions.aggregation_function import AggregationFunction
from Utils import get_aggregated_column, empty_data_frame, data_frames_union


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
        return list(range(math.ceil(max(aggregated_column)) * len(aggregated_column) + 1))

    def aggregate(self, data_frame: pd.DataFrame, aggregation_attribute_index: int) -> float:
        return sum(get_aggregated_column(data_frame, aggregation_attribute_index))

    def get_aggregation_packing(
            self,
            data_frame: pd.DataFrame,
            aggregation_attribute_index: int,
            lower_bound: float,
            upper_bound: float,
            possible_aggregations: List[float],
    ) -> pd.DataFrame:
        subsets_sizes: DefaultDict[Tuple[int, float], float] = defaultdict(lambda: float('-inf'))
        aggregation_packings: DefaultDict[Tuple[int, float], pd.DataFrame] = defaultdict(
            lambda: empty_data_frame(data_frame.columns)
        )

        set_first_aggregation_packing_and_subsets_sizes(
            aggregation_attribute_index,
            aggregation_packings,
            data_frame,
            possible_aggregations,
            subsets_sizes
        )

        for possible_aggregation in possible_aggregations:
            for j in range(1, len(data_frame)):
                current_iteration_tuple = (j, possible_aggregation)
                if (self.subsets_sizes[current_iteration_tuple] is not None
                        and self.aggregation_packings[current_iteration_tuple] is not None):
                    subsets_sizes[current_iteration_tuple] = self.subsets_sizes[current_iteration_tuple]
                    aggregation_packings[current_iteration_tuple] = self.aggregation_packings[current_iteration_tuple]
                    continue

                set_current_aggregation_packing_and_subsets_sizes(
                    aggregation_attribute_index,
                    aggregation_packings,
                    data_frame,
                    j,
                    possible_aggregation,
                    subsets_sizes,
                    current_iteration_tuple
                )

        return calculate_optimal_packing(
            aggregation_packings,
            data_frame,
            lower_bound,
            possible_aggregations,
            subsets_sizes,
            upper_bound
        )

    def __str__(self):
        return "SUM"


def set_first_aggregation_packing_and_subsets_sizes(
    aggregation_attribute_index: int,
    aggregation_packings: DefaultDict[Tuple[int, float], pd.DataFrame],
    data_frame: pd.DataFrame,
    possible_aggregations: List[float],
    subsets_sizes: DefaultDict[Tuple[int, float], float]
):
    first_row = data_frame.iloc[[0]]
    for possible_aggregation in possible_aggregations:
        if first_row.iloc[0, aggregation_attribute_index] == possible_aggregation:
            subsets_sizes[(0, possible_aggregation)] = 1
            aggregation_packings[(0, possible_aggregation)] = first_row


def set_current_aggregation_packing_and_subsets_sizes(
    aggregation_attribute_index: int,
    aggregation_packings: DefaultDict[Tuple[int, float], pd.DataFrame],
    data_frame: pd.DataFrame,
    j: int,
    possible_aggregation: float,
    subsets_sizes: DefaultDict[Tuple[int, float], float],
    current_iteration_tuple: Tuple[int, float]
):
    current_value = data_frame.iloc[j, aggregation_attribute_index]
    add_indicator_tuple = (j - 1, possible_aggregation - current_value)
    skip_indicator_tuple = (j - 1, possible_aggregation)

    add_current_row_indicator = (
            subsets_sizes[add_indicator_tuple] + 1
    )
    skip_current_row_indicator = subsets_sizes[skip_indicator_tuple]

    if add_current_row_indicator > skip_current_row_indicator:
        subsets_sizes[current_iteration_tuple] = add_current_row_indicator
        aggregation_packings[current_iteration_tuple] = data_frames_union(
            aggregation_packings[add_indicator_tuple], data_frame.iloc[[j]]
        )
    else:
        subsets_sizes[current_iteration_tuple] = skip_current_row_indicator
        aggregation_packings[current_iteration_tuple] = aggregation_packings[skip_indicator_tuple]


def calculate_optimal_packing(
    aggregation_packings: DefaultDict[Tuple[int, float], pd.DataFrame],
    data_frame: pd.DataFrame,
    lower_bound: float,
    possible_aggregations: List[float],
    subsets_sizes: DefaultDict[Tuple[int, float], float],
    upper_bound: float
):
    max_aggregation = float('-inf')
    result: pd.DataFrame = empty_data_frame(data_frame.columns)

    for possible_aggregation in possible_aggregations:
        if lower_bound <= possible_aggregation <= upper_bound:
            current_subset_tuple = (len(data_frame) - 1, possible_aggregation)
            current_subset_size = subsets_sizes[current_subset_tuple]

            if current_subset_size > max_aggregation:
                max_aggregation = current_subset_size
                result = aggregation_packings[current_subset_tuple]

    return result
