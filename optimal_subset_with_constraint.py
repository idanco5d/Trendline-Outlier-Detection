from typing import Dict, List, Union

import pandas as pd

from aggregations import AggregationFunction
from aggregations_mem import AggregationMem


def get_optimal_subset(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        agg: AggregationFunction
) -> (pd.DataFrame, pd.DataFrame):
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    # Dynamic programming table: key = agg value, value = maximal subset with agg value
    value_subsets: Dict[float, set] = {}
    for group_key, group_df in df.groupby(group_cols):  # groupby keys are sorted by default
        print(f"working on group: {group_key}")
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


def get_optimal_subset_mem_opt(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        Agg: AggregationMem
) -> (pd.DataFrame, pd.DataFrame):
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    print("agg result before removal:")
    print(df.groupby(group_cols)[agg_col].sum())
    # Dynamic programming table: key = agg value, value = maximal subset with agg value
    value_subsets: Dict[float, Dict[int, tuple]] = {} # agg_value -> dict of group_id to (size, agg_val) (for the previous groups)
    group_to_agg = {}
    for group_key, group_df in df.groupby(group_cols):  # groupby keys are sorted by default
        print(f"working on group: {group_key}")
        agg = Agg()
        group_to_agg[group_key] = agg # save it for later
        current_group_subsets = agg.compute_max_subset_sizes(group_df, agg_col) # dict of agg_val: maximal subset size
        new_value_subsets: Dict[float, Dict[int, tuple]] = {} # agg_val of current ri -> {group_id -> (size, agg_val)}

        for value, subset_size in current_group_subsets.items():
            previous_groups_subset_sizes_and_values = get_maximal_set_sizes_with_upper_bound(value_subsets, value)
            previous_groups_subset_sizes_and_values[group_key] = (subset_size, value)
            new_value_subsets[value] = previous_groups_subset_sizes_and_values

        for value in value_subsets.keys():
            if value not in new_value_subsets.keys():
                new_value_subsets[value] = value_subsets[value]

        value_subsets = new_value_subsets.copy()

    # Next - get the set of indices from the group ids, agg values and sizes.
    gid_to_size_and_val = get_maximal_set_sizes_with_upper_bound(value_subsets)
    optimal_subset = []
    for gid in gid_to_size_and_val.keys():
        req_size, req_value = gid_to_size_and_val[gid]
        group_subset = group_to_agg[gid].get_subset_for_value(req_value)
        if len(group_subset) != req_size:
            raise Exception(f"mismatch in subset sizes for group: {gid}")
        optimal_subset.extend(group_subset)
    #optimal_subset = list(get_maximal_set_with_upper_bound(value_subsets))
    subset_df = df.iloc[optimal_subset]
    removed_df = df.loc[~df.index.isin(optimal_subset)]
    print("agg result after removal:")
    print(subset_df.groupby(group_cols)[agg_col].sum())

    return subset_df, removed_df


def get_maximal_set_sizes_with_upper_bound(value_sets: Dict[float, set], upper_bound: float = None) -> Dict[int, tuple]:
    # value_sets: dict of {agg_value : {group_id: (size, agg_val)}}
    max_repair_size = 0
    repair_sizes_and_values = {}
    #print(f"upper bound: {upper_bound} value_sets: {value_sets}")
    for current_value, gid_to_size_and_val in value_sets.items():
        current_size = sum([gid_to_size_and_val[group_id][0] for group_id in gid_to_size_and_val])
        if (upper_bound is None or current_value <= upper_bound) and current_size > max_repair_size:
            max_repair_size = current_size
            repair_sizes_and_values = gid_to_size_and_val.copy()
    return repair_sizes_and_values