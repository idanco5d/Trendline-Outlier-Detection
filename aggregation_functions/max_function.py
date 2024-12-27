from typing import List

import pandas as pd

from aggregation_functions.aggregation_function import AggregationFunction
from utils import empty_data_frame, get_aggregated_column, get_list_of_column_values


class MaxFunction(AggregationFunction):
    def get_possible_subsets_aggregations(
            self, data_frame: pd.DataFrame, aggregation_attribute_index: int
    ) -> List[float]:
        return get_list_of_column_values(data_frame, aggregation_attribute_index)

    def aggregate(self, data_frame: pd.DataFrame, aggregation_attribute_index: int) -> float:
        return float(get_aggregated_column(data_frame, aggregation_attribute_index).max())

    def get_aggregation_packing(
            self,
            data_frame: pd.DataFrame,
            aggregation_attribute_index: int,
            lower_bound: float,
            upper_bound: float,
            possible_aggregations: List[float],
    ) -> pd.DataFrame:
        filtered_df = data_frame.loc[data_frame.iloc[:, aggregation_attribute_index].le(upper_bound)]
        max_value = filtered_df.iloc[:, aggregation_attribute_index].max()

        if lower_bound > max_value:
            return empty_data_frame(data_frame.columns)

        return filtered_df

    def __str__(self):
        return "MAX"
