from typing import Dict, List

import pandas as pd

from aggregation_functions.aggregation_function import AggregationFunction
from utils import empty_data_frame, get_aggregated_column


class CountDistinctFunction(AggregationFunction):
    def get_possible_subsets_aggregations(
            self, data_frame: pd.DataFrame, aggregation_attribute_index: int
    ) -> List[float]:
        return list(range(len(get_aggregated_column(data_frame, aggregation_attribute_index).unique()) + 1))

    def aggregate(self, data_frame: pd.DataFrame, aggregation_attribute_index: int) -> float:
        return get_aggregated_column(data_frame, aggregation_attribute_index).nunique()

    def get_aggregation_packing(
            self,
            data_frame: pd.DataFrame,
            aggregation_attribute_index: int,
            lower_bound: float,
            upper_bound: float,
            possible_aggregations: List[float],
    ) -> pd.DataFrame:
        aggregation_col = data_frame.columns[aggregation_attribute_index]
        count_distinct = data_frame[aggregation_col].nunique()

        if count_distinct < lower_bound:
            return empty_data_frame(data_frame.columns)

        num_to_remove = count_distinct - upper_bound
        value_counts = data_frame[aggregation_col].value_counts()
        # If num_to_remove <= 0, nsmallest returns an empty list
        least_common = value_counts.nsmallest(num_to_remove).index
        return data_frame.loc[~data_frame[aggregation_col].isin(least_common)]

    def __str__(self):
        return "COUNT_DISTINCT"
