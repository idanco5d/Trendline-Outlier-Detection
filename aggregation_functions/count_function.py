from typing import List

import pandas as pd

from aggregation_functions.aggregation_function import AggregationFunction
from Utils import empty_data_frame, get_aggregated_column


class CountFunction(AggregationFunction):
    def get_possible_subsets_aggregations(
            self, data_frame: pd.DataFrame, aggregation_attribute_index: int
    ) -> List[float]:
        return list(range(len(data_frame) + 1))

    def aggregate(self, data_frame: pd.DataFrame, aggregation_attribute_index: int) -> float:
        return len(get_aggregated_column(data_frame, aggregation_attribute_index))

    def get_aggregation_packing(
            self,
            data_frame: pd.DataFrame,
            aggregation_attribute_index: int,
            lower_bound: float,
            upper_bound: float,
            possible_aggregations: List[float],
    ) -> pd.DataFrame:
        empty_frame = empty_data_frame(data_frame.columns)
        aggregated_column = get_aggregated_column(data_frame, aggregation_attribute_index)
        aggregated_column_size = len(aggregated_column)

        if aggregated_column_size < lower_bound:
            return empty_frame

        amount_tuples_to_return = int(min(upper_bound, aggregated_column_size))

        return data_frame.head(amount_tuples_to_return)

    def __str__(self):
        return "COUNT"
