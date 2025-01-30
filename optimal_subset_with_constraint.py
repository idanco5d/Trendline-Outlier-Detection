from typing import Dict, List, Callable, Union

import pandas as pd


def get_optimal_subset(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        agg: Callable[[pd.DataFrame, str], Dict[float, set]]
) -> (pd.DataFrame, pd.DataFrame):
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    # Dynamic programming table: key = agg value, value = maximal subset with agg value
    value_subsets: Dict[float, set] = {}
    for group_key, group_df in df.groupby(group_cols):  # groupby keys are sorted by default
        current_group_subsets = agg(group_df, agg_col)
        new_value_subsets: Dict[float, set] = {}

        for value, subset in current_group_subsets.items():
            previous_groups_subset = get_maximal_set_with_upper_bound(value_subsets, value)
            new_value_subsets[value] = previous_groups_subset | subset

        for value, subset in value_subsets.items():
            if value not in new_value_subsets.keys():
                new_value_subsets[value] = value_subsets[value]

        value_subsets = new_value_subsets

    optimal_subset = list(get_maximal_set_with_upper_bound(value_subsets))
    subset_df = df.iloc[optimal_subset]
    removed_df = df.loc[~df.index.isin(optimal_subset)]

    return subset_df, removed_df


def get_maximal_set_with_upper_bound(value_sets: Dict[float, set], upper_bound: float = None) -> set:
    maximal_set = set()

    for current_value, current_set in value_sets.items():
        if (upper_bound is None or current_value <= upper_bound) and len(current_set) > len(maximal_set):
            maximal_set = current_set

    return maximal_set
