from typing import Dict, List, Union, Tuple

import pandas as pd
from sortedcontainers import SortedDict

from aggregations_pruning import AggregationPruningFunction


def get_optimal_subset_pruning(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        agg: AggregationPruningFunction,
        max_removed: int = None,
        parallelize: bool = False
) -> (pd.DataFrame, pd.DataFrame):
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    # Dynamic programming table:
    # key = agg value
    # Value = maximal subset with agg value & number of removed tuples
    value_subsets: SortedDict[float, Tuple[set, int]] = SortedDict()

    for group_key, group_df in df.groupby(group_cols):  # groupby keys are sorted by default
        print(f"working on group: {group_key}")
        min_subset_size = len(group_df) - max_removed if max_removed is not None else None
        current_group_subsets = agg(group_df, agg_col, min_subset_size, parallelize)
        new_value_subsets: Dict[float, Tuple[set, int]] = {}

        for value, subset in current_group_subsets.items():
            previous_groups_subset, previous_removed_count = get_maximal_set_with_upper_bound(
                value_subsets, value
            )
            new_subset = previous_groups_subset | subset
            new_removed_count = previous_removed_count + len(group_df) - len(subset)
            if new_removed_count <= max_removed:
                new_value_subsets[value] = (new_subset, new_removed_count)

        for value in value_subsets.keys():
            if value not in new_value_subsets.keys():
                new_value_subsets[value] = value_subsets[value]

        # More pruning!
        # If we have v1, v2 s.t. v1<=v2 and size(value_subsets[v1])>=size(value_subsets[v2]), no need to save v2.
        # We would always prefer the solution for v1.
        max_keep_size = 0
        pruned_value_subsets = {}
        for x in sorted(new_value_subsets.keys()):
            subset, removed_count = new_value_subsets[x]
            keep_size = len(subset)
            if keep_size > max_keep_size:
                pruned_value_subsets[x] = (subset, removed_count)
                max_keep_size = keep_size

        value_subsets = SortedDict(pruned_value_subsets)

        # value_subsets = new_value_subsets

    optimal_subset = list(get_maximal_set_with_upper_bound(value_subsets)[0])
    subset_df = df.iloc[optimal_subset]
    removed_df = df.loc[~df.index.isin(optimal_subset)]

    return subset_df, removed_df


def get_maximal_set_with_upper_bound(
        value_sets: SortedDict[float, Tuple[set, int]], upper_bound: float = None
) -> Tuple[set, int]:
    if upper_bound is not None:
        index = value_sets.bisect_left(upper_bound)
        # should include keys[index]? yes, if it is still <= upper bound (i.e., equal to it)
        if index < len(value_sets) and value_sets.keys()[index] <= upper_bound:
            index += 1
        values_to_consider = value_sets.keys()[:index]
    else:
        values_to_consider = value_sets.keys()
    maximal_set = set()
    removed_count = 0

    for current_value in values_to_consider:
        current_set, current_removed_count = value_sets[current_value]
        if len(current_set) > len(maximal_set):
            maximal_set = current_set
            removed_count = current_removed_count
    return maximal_set, removed_count
