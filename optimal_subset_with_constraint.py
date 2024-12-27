from typing import List, Dict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from aggregation_functions.aggregation_function import AggregationFunction
from utils import list_of_empty_dictionaries, get_group_by_key, empty_data_frame, data_frames_union


def calculate_optimal_subset_with_constraint(
    grouped_rows: DataFrameGroupBy,
    aggregation_function: AggregationFunction,
    aggregation_attribute_index: int
) -> pd.DataFrame:
    grouping_values = iter(grouped_rows.groups.keys())
    minimal_grouping_value_group = get_group_by_key(grouped_rows, next(grouping_values))
    solutions = list_of_empty_dictionaries(
        len(grouped_rows.groups.keys()), minimal_grouping_value_group.columns
    )

    calculate_minimal_value_group_solution(
        solutions,
        aggregation_function,
        minimal_grouping_value_group,
        aggregation_attribute_index
    )

    for current_index, grouping_value in enumerate(grouping_values, start=1):
        current_value_group = get_group_by_key(grouped_rows, grouping_value)
        current_possible_aggregations = aggregation_function.get_possible_subsets_aggregations(
            current_value_group,
            aggregation_attribute_index
        )
        possible_aggregations_length = len(current_possible_aggregations)
        solution_max_size_per_upper_bound: List[int] = [0 for _ in range(possible_aggregations_length)]

        for i in range(possible_aggregations_length):
            lower_bound = current_possible_aggregations[i]
            for j in range(i, possible_aggregations_length):
                upper_bound = current_possible_aggregations[j]

                current_bounds_solution = calculate_current_bounds_solution(aggregation_function, current_value_group,
                                                                            aggregation_attribute_index, lower_bound,
                                                                            upper_bound, current_possible_aggregations,
                                                                            solutions, current_index)
                current_bounds_solution_length = len(current_bounds_solution)

                if current_bounds_solution_length > solution_max_size_per_upper_bound[j]:
                    solution_max_size_per_upper_bound[j] = current_bounds_solution_length
                    solutions[current_index][upper_bound] = current_bounds_solution

    final_solution_candidates = solutions[-1].values()
    if len(final_solution_candidates) == 0:
        return empty_data_frame(minimal_grouping_value_group.columns)
    return max(final_solution_candidates, key=lambda df: df.size).sort_index()


def calculate_minimal_value_group_solution(
        solutions: List[Dict[float, pd.DataFrame]],
        aggregation_function: AggregationFunction,
        minimal_value_group: pd.DataFrame,
        aggregation_attribute_index: int
):
    possible_aggregations = aggregation_function.get_possible_subsets_aggregations(
        minimal_value_group, aggregation_attribute_index
    )
    min_possible_aggregation = min(possible_aggregations)

    for upper_bound in possible_aggregations:
        solutions[0][upper_bound] = aggregation_function.get_aggregation_packing(
            minimal_value_group,
            aggregation_attribute_index,
            min_possible_aggregation,
            upper_bound,
            possible_aggregations
        )


def calculate_current_bounds_solution(
        aggregation_function: AggregationFunction,
        current_value_group: pd.DataFrame,
        aggregation_attribute_index: int,
        lower_bound: float,
        upper_bound: float,
        possible_aggregations: List[float],
        solutions: List[Dict[float, pd.DataFrame]],
        current_index: int,
) -> pd.DataFrame:
    aggregation_packing = aggregation_function.get_aggregation_packing(
        current_value_group,
        aggregation_attribute_index,
        lower_bound,
        upper_bound,
        possible_aggregations
    )

    if len(aggregation_packing) == 0:
        current_bounds_solution = solutions[current_index - 1][lower_bound]
    else:
        aggregation = aggregation_function.aggregate(aggregation_packing, aggregation_attribute_index)
        current_bounds_solution = data_frames_union(
            solutions[current_index - 1][aggregation], aggregation_packing
        )

    return current_bounds_solution
