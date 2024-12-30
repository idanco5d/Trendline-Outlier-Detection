from typing import List, Dict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from aggregations.aggregation import Aggregation
from utils import list_of_empty_dictionaries, get_group_by_key, empty_data_frame, data_frames_union


def calculate_optimal_subset_with_constraint(
    grouped_rows: DataFrameGroupBy,
    agg: Aggregation,
    agg_col: str
) -> pd.DataFrame:
    grouping_values = iter(grouped_rows.groups.keys())
    minimal_grouping_value_group = get_group_by_key(grouped_rows, next(grouping_values))
    solutions = list_of_empty_dictionaries(
        len(grouped_rows.groups.keys()), minimal_grouping_value_group.columns
    )

    calculate_minimal_value_group_solution(
        solutions,
        agg,
        minimal_grouping_value_group,
        agg_col
    )

    for current_index, grouping_value in enumerate(grouping_values, start=1):
        current_value_group = get_group_by_key(grouped_rows, grouping_value)
        current_possible_aggregations = agg.get_possible_subsets_aggregations(
            current_value_group,
            agg_col
        )
        possible_aggregations_length = len(current_possible_aggregations)
        solution_max_size_per_upper_bound: List[int] = [0 for _ in range(possible_aggregations_length)]

        for i in range(possible_aggregations_length):
            lower_bound = current_possible_aggregations[i]
            for j in range(i, possible_aggregations_length):
                upper_bound = current_possible_aggregations[j]

                current_bounds_solution = calculate_current_bounds_solution(
                    agg, current_value_group, agg_col, lower_bound, upper_bound, solutions, current_index
                )
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
        agg: Aggregation,
        minimal_value_group: pd.DataFrame,
        agg_col: str
):
    possible_aggregations = agg.get_possible_subsets_aggregations(
        minimal_value_group, agg_col
    )
    min_possible_aggregation = min(possible_aggregations)

    for upper_bound in possible_aggregations:
        solutions[0][upper_bound] = agg.get_aggregation_packing(
            minimal_value_group,
            agg_col,
            min_possible_aggregation,
            upper_bound
        )


def calculate_current_bounds_solution(
        agg: Aggregation,
        current_value_group: pd.DataFrame,
        agg_col: str,
        lower_bound: float,
        upper_bound: float,
        solutions: List[Dict[float, pd.DataFrame]],
        current_index: int,
) -> pd.DataFrame:
    aggregation_packing = agg.get_aggregation_packing(
        current_value_group,
        agg_col,
        lower_bound,
        upper_bound
    )

    if len(aggregation_packing) == 0:
        current_bounds_solution = solutions[current_index - 1][lower_bound]
    else:
        aggregation = agg.aggregate(aggregation_packing, agg_col)
        current_bounds_solution = data_frames_union(
            solutions[current_index - 1][aggregation], aggregation_packing
        )

    return current_bounds_solution
