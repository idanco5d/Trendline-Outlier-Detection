from typing import List

import pandas as pd

from aggregation_functions.aggregation_function import AggregationFunction
from Utils import empty_data_frame, get_aggregated_column, get_list_of_column_values


class MaxFunction(AggregationFunction):
    def get_possible_subsets_aggregations(
            self, data_frame: pd.DataFrame, aggregation_attribute_index: int
    ) -> List[float]:
        return get_list_of_column_values(data_frame, aggregation_attribute_index)

    def aggregate(self, data_frame: pd.DataFrame, aggregation_attribute_index: int) -> float:
        return max(get_aggregated_column(data_frame, aggregation_attribute_index))

    def get_aggregation_packing(
            self,
            data_frame: pd.DataFrame,
            aggregation_attribute_index: int,
            lower_bound: float,
            upper_bound: float,
            possible_aggregations: List[float],
    ) -> pd.DataFrame:
        result = empty_data_frame(data_frame.columns)
        max_value = float('-inf')

        for index, dataset_tuple in data_frame.iterrows():
            current_value = dataset_tuple.iloc[aggregation_attribute_index]
            if current_value <= upper_bound:
                result.loc[index] = dataset_tuple
            if current_value > max_value:
                max_value = current_value

        if max_value < lower_bound:
            return empty_data_frame(data_frame.columns)
        return result

    def __str__(self):
        return "MAX"
