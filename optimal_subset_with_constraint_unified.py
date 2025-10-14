from sortedcontainers import SortedDict
import pandas as pd
from typing import List, Union, Type

from aggregations_mem import AggregationMem
from pubsub import get_pubsub_topic, with_redis_pubsub


def update_H_with_pruning(F, H, group_id):
    """
    :param F: dictionary from agg value to count of remaining tuples (for group i)
    :param H: sorted dict of agg value (x) to: (count, [(agg_value, group_id)]) for groups 1..i-1, such that agg value of group i-1 = x)
    :param group_id: value of group by column that represents group i.
    :return:
    """
    agg_values = sorted(F.keys(), reverse=True)  # sort feasible aggregation values from large to small
    # for agg_value in range(len(F) - 1, 0, -1):
    for agg_value in agg_values:
        if F[agg_value] > 0:
            index = H.bisect_right(agg_value)
            largest_below_agg_value = H.keys()[index-1]
            candidate_repair_size = H[largest_below_agg_value][0] + F[agg_value]
            if (agg_value not in H) or (candidate_repair_size > H[agg_value][0]):
                new_list = H[largest_below_agg_value][1].copy()
                new_list.insert(0, (agg_value, group_id))
                H[agg_value] = (candidate_repair_size, new_list)
    return H


def update_H_no_pruning(F, H, group_id):
    """
    :param F: dictionary from agg value to count of remaining tuples (for group i)
    :param H: sorted dict of agg value (x) to: (count, [(agg_value, group_id)]) for groups 1..i-1, such that agg value of group i-1 = x)
    :param group_id: value of group by column that represents group i.
    :return:
    """
    agg_values = sorted(F.keys(), reverse=False)  # sort feasible aggregation values from large to small

    H_keys = sorted(H.keys(), reverse=False) # keep frozen for the iteration

    previous_max_repair_size = 0
    previous_max_repair_group_values = []
    H_index = 0
    for agg_value in agg_values:
        if F[agg_value] <= 0:
            continue
        while H_index < len(H_keys) and H_keys[H_index] <= agg_value:
            repair_size = H[H_keys[H_index]][0]
            if repair_size > previous_max_repair_size:
                previous_max_repair_size = repair_size
                previous_max_repair_group_values = H[H_keys[H_index]][1].copy()
            H_index += 1
        candidate_repair_size = previous_max_repair_size + F[agg_value]
        if (agg_value not in H) or (candidate_repair_size > H[agg_value][0]):
            new_list = previous_max_repair_group_values.copy()
            new_list.insert(0, (agg_value, group_id))
            H[agg_value] = (candidate_repair_size, new_list)
    return H


def prune_H(H, max_removed=None, sum_of_group_sizes=None):
    """
    If x1<=x2 and H(x1)>=H(x2), keep only x1, and prune x2.
    :param H: sorted dict of agg value (x) to: (count, [(agg_value, group_id)]) for groups 1..i, such that agg value of group i = x)
    """
    max_count = -1
    newH = {}
    for option in H.keys():
        if option > 0 and max_removed is not None:
            # compute removal from groups 1,.., i. If it's too large, no need to remember this option.
            if (sum_of_group_sizes - H[option][0]) > max_removed:
                continue
        if H[option][0] > max_count:
            newH[option] = H[option]
            max_count = H[option][0]
    return SortedDict(newH)


def prune_H_by_max_removed(H, max_removed, sum_of_group_sizes=None):
    """
    If x1<=x2 and H(x1)>=H(x2), keep only x1, and prune x2.
    :param H: sorted dict of agg value (x) to: (count, [(agg_value, group_id)]) for groups 1..i, such that agg value of group i = x)
    """
    newH = {}
    for option in H.keys():
        # compute removal from groups 1,.., i-1. If it's too large, no need to remember this option.
        if (sum_of_group_sizes - H[option][0]) > max_removed:
            continue
        newH[option] = H[option]
    return SortedDict(newH)

@with_redis_pubsub
def _notify_intermediate_result(data: dict) -> dict:
    return data

def get_optimal_subset_F_first(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        #agg_func_str: str,
        Agg: Type[AggregationMem],
        max_removed: int = None,
        prune_dp_by_max_removed: int = None,
        prune_h: bool = False,
        time_cutoff_seconds: int = None,
        htrack_file=None,
        **kwargs
) -> (pd.DataFrame, pd.DataFrame):
    print(len(df))
    print("mem opt, F first")
    if max_removed is not None:
        print(f"prune agg pack: {max_removed}")
    if prune_dp_by_max_removed is not None:
        print(f"prune dp: {prune_dp_by_max_removed}")
    print(f"prune h: {prune_h}")
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    print("agg result before repair:")
    print(df.groupby(group_cols)[agg_col].agg(['sum', 'count', 'mean', 'median', 'max']))

    output = {}

    H = SortedDict()
    H[0] = (0, [])  # first element is the amount of items, the second is the aggregation value in each key group
    group_keys = []

    aggs = {}
    group_sizes = {}
    # First compute F (realizable aggregations and max subset size) for each group.
    for group_key, group_df in df.groupby(group_cols):  # groupby keys are sorted by default
        print(f"working on group: {group_key}")
        agg = Agg()
        group_output = agg.compute_max_subset_sizes(group_df, agg_col, max_removed, time_cutoff_seconds)
        if kwargs.get('notify'):
            group_name = "_".join(str(g) for g in group_key)
            _notify_intermediate_result(
                topic=get_pubsub_topic(task_id=kwargs.get('task_id')),
                data={group_name: group_output}
            )
        output[group_key] = group_output
        aggs[group_key] = agg
        group_keys.append(group_key)
        group_sizes[group_key] = len(group_df)
    # Next, compute the solution (main DP).
    sum_of_group_sizes = 0

    # create a new file for tracking H sizes
    if htrack_file is not None:
        htrack = open(htrack_file, "w")
        htrack.close()

    for group_key in group_keys:
        print(f"merging + pruning group: {group_key}")
        sum_of_group_sizes += group_sizes[group_key]
        if prune_h:
            H = update_H_with_pruning(output[group_key], H, group_key)
            H = prune_H(H, prune_dp_by_max_removed, sum_of_group_sizes)
        else:
            H = update_H_no_pruning(output[group_key], H, group_key)
            if prune_dp_by_max_removed is not None:
                H = prune_H_by_max_removed(H, prune_dp_by_max_removed, sum_of_group_sizes)
        # output the num keys in H
        if htrack_file is not None:
            with open(htrack_file, "a") as out:
                out.write(f"{group_key}, num_keys in H: {len(H)}\r\n")


    ids_to_keep = []
    if prune_h:
        # We don't need to search for the best solution in H because of the pruning.
        # The solution with the largest x value will be the largest repair.
        largest_x = H.keys()[-1]
        print(f"largest_x: {largest_x}, H[largest_x]={H[largest_x]}")
        agg_values_and_group_keys = H[largest_x][1]
    else:
        # No pruning - search for the best solution in H
        best_repair_size = 0
        agg_values_and_group_keys = None
        for x in H:
            repair_size, group_values = H[x]
            if repair_size > best_repair_size:
                best_repair_size = repair_size
                agg_values_and_group_keys = group_values

    for agg_value, group_key in agg_values_and_group_keys:
        print(f"find subset for group {group_key} with agg value {agg_value}")
        ids_to_keep.extend(aggs[group_key].get_subset_for_value(agg_value))

    subset_df = df.iloc[ids_to_keep]
    removed_df = df.loc[~df.index.isin(ids_to_keep)]
    print("agg result after repair:")
    print(subset_df.groupby(group_cols)[agg_col].agg(['sum', 'count', 'mean', 'median', 'max']))

    return subset_df, removed_df
