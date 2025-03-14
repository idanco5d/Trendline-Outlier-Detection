from typing import Dict, List, Callable, Union, Tuple

import pandas as pd


def get_optimal_subset_pruning(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        agg: Callable[[pd.DataFrame, str, int], Dict[float, set]],
        max_removed: int = None
) -> (pd.DataFrame, pd.DataFrame):
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    # Dynamic programming table:
    # key = agg value
    # Value = maximal subset with agg value & number of removed tuples
    value_subsets: Dict[float, Tuple[set, int]] = {}

    for group_key, group_df in df.groupby(group_cols):  # groupby keys are sorted by default
        print(f"working on group: {group_key}")
        min_subset_size = len(group_df) - max_removed if max_removed is not None else None
        current_group_subsets = agg(group_df, agg_col, min_subset_size)
        new_value_subsets: Dict[float, Tuple[set, int]] = {}

        for value, subset in current_group_subsets.items():
            previous_groups_subset, previous_removed_count = get_maximal_set_with_upper_bound(value_subsets, value)
            new_subset = previous_groups_subset | subset
            new_removed_count = previous_removed_count + len(group_df) - len(subset)
            if new_removed_count <= max_removed:
                new_value_subsets[value] = (new_subset, new_removed_count)

        for value in value_subsets.keys():
            if value not in new_value_subsets.keys():
                new_value_subsets[value] = value_subsets[value]

        value_subsets = new_value_subsets

    optimal_subset = list(get_maximal_set_with_upper_bound(value_subsets)[0])
    subset_df = df.iloc[optimal_subset]
    removed_df = df.loc[~df.index.isin(optimal_subset)]

    return subset_df, removed_df


def get_maximal_set_with_upper_bound(value_sets: Dict[float, Tuple[set, int]], upper_bound: float = None) -> Tuple[
    set, int]:
    maximal_set = set()
    removed_count = 0

    for current_value, (current_set, current_removed_count) in value_sets.items():
        if (upper_bound is None or current_value <= upper_bound) and len(current_set) > len(maximal_set):
            maximal_set = current_set
            removed_count = current_removed_count

    return maximal_set, removed_count
