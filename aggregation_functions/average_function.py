from collections import defaultdict
from typing import Tuple, DefaultDict, List

import pandas as pd

from aggregation_functions.aggregation_function import AggregationFunction
from aggregation_functions.count_function import CountFunction
from aggregation_functions.sum_function import SumFunction
from Utils import get_aggregated_column, empty_data_frame, data_frames_union


class AverageFunction(AggregationFunction):
    def __init__(self):
        super().__init__()
        self.sum_possible_aggregations: List[float] = []
        self.count_possible_aggregations: List[float] = []
        self.subsets_existence_with_size: DefaultDict[Tuple[int, float, float], float | None] = defaultdict(lambda: None)
        self.aggregation_packings: DefaultDict[Tuple[int, float, float], pd.DataFrame | None] = defaultdict(lambda: None)

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
        return sum(aggregation_column) / len(aggregation_column)

    def get_aggregation_packing(
            self,
            data_frame: pd.DataFrame,
            aggregation_attribute_index: int,
            lower_bound: float,
            upper_bound: float,
            possible_aggregations: List[float],
    ) -> pd.DataFrame:
        subsets_existence_with_size: DefaultDict[Tuple[int, float, float], float] = defaultdict(lambda: float('-inf'))
        aggregation_packings: DefaultDict[Tuple[int, float, float], pd.DataFrame] = defaultdict(
            lambda: empty_data_frame(data_frame.columns)
        )

        set_first_aggregation_packing_and_subsets_existence(
            aggregation_attribute_index,
            aggregation_packings,
            data_frame,
            subsets_existence_with_size
        )

        for j in range(1, len(data_frame)):
            for sum_aggregation in self.sum_possible_aggregations:
                for count_aggregation in self.count_possible_aggregations:
                    if count_aggregation == 0:
                        continue

                    current_iteration_tuple = (j, sum_aggregation, count_aggregation)
                    if self.subsets_existence_with_size[current_iteration_tuple] is not None and \
                            self.aggregation_packings[current_iteration_tuple] is not None:
                        subsets_existence_with_size[current_iteration_tuple] = (
                            self.subsets_existence_with_size)[current_iteration_tuple]
                        aggregation_packings[(j, sum_aggregation, count_aggregation)] = (
                            self.aggregation_packings)[current_iteration_tuple]
                        continue

                    self.set_current_aggregation_packings_and_subsets_existence(
                        aggregation_attribute_index,
                        aggregation_packings,
                        count_aggregation,
                        data_frame,
                        j,
                        subsets_existence_with_size,
                        sum_aggregation,
                        current_iteration_tuple
                    )

        return self.calculate_optimal_packing(
            aggregation_packings,
            data_frame,
            lower_bound,
            subsets_existence_with_size,
            upper_bound
        )

    def set_current_aggregation_packings_and_subsets_existence(
            self,
            aggregation_attribute_index: int,
            aggregation_packings: DefaultDict[Tuple[int, float, float], pd.DataFrame],
            count_aggregation: float,
            data_frame: pd.DataFrame,
            j: int,
            subsets_existence_with_size: DefaultDict[Tuple[int, float, float], float],
            sum_aggregation: float,
            current_iteration_tuple: Tuple[int, float, float]
    ):
        skip_indicator_tuple = (j - 1, sum_aggregation, count_aggregation)

        if count_aggregation > j + 1:
            subsets_existence_with_size[current_iteration_tuple] = float('-inf')
            self.subsets_existence_with_size[current_iteration_tuple] = float('-inf')
        else:
            current_value = data_frame.iloc[j, aggregation_attribute_index]
            add_indicator_tuple = (j - 1, sum_aggregation - current_value, count_aggregation - 1)

            add_current_row_indicator = (
                    subsets_existence_with_size[add_indicator_tuple] + 1
            )
            skip_current_row_indicator = (
                subsets_existence_with_size[skip_indicator_tuple]
            )

            if add_current_row_indicator > skip_current_row_indicator:
                subsets_existence_with_size[current_iteration_tuple] = add_current_row_indicator
                self.subsets_existence_with_size[current_iteration_tuple] = add_current_row_indicator
                aggregation_packings[current_iteration_tuple] = data_frames_union(
                    aggregation_packings[add_indicator_tuple],
                    data_frame.iloc[[j]]
                )
                self.aggregation_packings[current_iteration_tuple] = aggregation_packings[current_iteration_tuple]
            else:
                subsets_existence_with_size[current_iteration_tuple] = skip_current_row_indicator
                self.subsets_existence_with_size[current_iteration_tuple] = skip_current_row_indicator
                aggregation_packings[current_iteration_tuple] = aggregation_packings[
                    skip_indicator_tuple
                ]
                self.aggregation_packings[current_iteration_tuple] = aggregation_packings[current_iteration_tuple]

    def calculate_optimal_packing(
            self,
            aggregation_packings: DefaultDict[Tuple[int, float, float], pd.DataFrame],
            data_frame: pd.DataFrame,
            lower_bound: float,
            subsets_existence_with_size: DefaultDict[Tuple[int, float, float], float],
            upper_bound: float
    ):
        max_subset_size = float('-inf')
        result: pd.DataFrame = empty_data_frame(data_frame.columns)

        for sum_aggregation in self.sum_possible_aggregations:
            for count_aggregation in self.count_possible_aggregations:
                if count_aggregation == 0:
                    continue
                if lower_bound <= (sum_aggregation / count_aggregation) <= upper_bound:
                    current_subset_tuple = (len(data_frame) - 1, sum_aggregation, count_aggregation)
                    current_subset_size = subsets_existence_with_size[current_subset_tuple]

                    if current_subset_size > max_subset_size:
                        max_subset_size = current_subset_size
                        result = aggregation_packings[current_subset_tuple]

        return result

    def __str__(self):
        return "AVG"


def set_first_aggregation_packing_and_subsets_existence(
        aggregation_attribute_index: int,
        aggregation_packings: DefaultDict[Tuple[int, float, float], pd.DataFrame],
        data_frame: pd.DataFrame,
        subsets_existence_with_size: DefaultDict[Tuple[int, float, float], float]
):
    for j in range(len(data_frame)):
        subsets_existence_with_size[(j, 0, 0)] = 0

    first_row = data_frame.iloc[[0]]
    subsets_existence_with_size[(0, first_row.iloc[0, aggregation_attribute_index], 1)] = 1
    aggregation_packings[(0, first_row.iloc[0, aggregation_attribute_index], 1)] = first_row
