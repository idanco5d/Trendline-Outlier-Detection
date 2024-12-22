from typing import Dict, List

import pandas as pd

from aggregation_functions.aggregation_function import AggregationFunction
from Utils import empty_data_frame, get_aggregated_column


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
        result = data_frame.copy()

        while True:
            aggregated_column = get_aggregated_column(result, aggregation_attribute_index)
            values_count: Dict[float, int] = aggregated_column.value_counts().to_dict()
            count_distinct: int = aggregated_column.nunique()

            if count_distinct <= upper_bound:
                break

            counts = values_count.values()
            if len(counts) == 0:
                break

            min_count = min(counts)
            for val, count in values_count.items():
                if count == min_count:
                    result = result[~(aggregated_column == val)]
                    break

        if count_distinct < lower_bound:
            return empty_data_frame(data_frame.columns)

        return result

    def __str__(self):
        return "COUNT_DISTINCT"
