from multiprocessing import Pool

from sortedcontainers import SortedDict
import pandas as pd
from typing import Dict, List, Union, Type

from aggregations_mem import AggregationMem


def update_H(F, H, group_id):
    """
    :param F: agg value to count of remaining tuples (for group i)
    :param H: sorted dict of agg value (x) to: (count, [(agg_value, group_id)]) for groups 1..i-1, such that agg value of group i-1 = x)
    :param group_id: value of group by column that represents group i.
    :return:
    """
    for agg_value in range(len(F) - 1, 0, -1):
        if F[agg_value] > 0:
            index = H.bisect_right(agg_value)
            largest_below_agg_value = H.keys()[index-1]
            candidate_repair_size = H[largest_below_agg_value][0] + F[agg_value]
            if (agg_value not in H) or (candidate_repair_size > H[agg_value][0]):
                new_list = H[largest_below_agg_value][1].copy()
                new_list.insert(0, (agg_value, group_id))
                H[agg_value] = (candidate_repair_size, new_list)
    return H


def prune_H(H):
    """
    If x1<=x2 and H(x1)>=H(x2), keep only x1, and prune x2.
    :param H: agg value to count of remaining tuples (for groups 1... i-1, such that agg(group i-1) = agg value)
    """
    max_count = -1
    newH = {}
    for option in H.keys():
        if H[option][0] > max_count:
            newH[option] = H[option]
            max_count = H[option][0]
    return SortedDict(newH)

def compute_F_for_group(Agg: Type[AggregationMem], agg_col: str, group_tuple: tuple):
    group_key, group_df = group_tuple
    print(f"working on group: {group_key}")  # a bit misleading when being parallelized
    agg = Agg()
    return group_key, agg.compute_max_subset_sizes(group_df, agg_col), agg

def get_optimal_subset_F_first(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        #agg_func_str: str,
        Agg: Type[AggregationMem],
        #max_removed: int = None,
        parallel: bool = False
) -> (pd.DataFrame, pd.DataFrame):
    print(len(df))
    print("mem opt")
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    print("agg result before repair:")
    print(df.groupby(group_cols)[agg_col].agg(['sum', 'count', 'mean', 'median']))

    output = {}

    H = SortedDict()
    H[0] = (0, [])  # first element is the amount of items, the second is the sum in each key group
    keys = []

    aggs = {}
    # First compute F (realizable aggregations and max subset size) for each group.

    groups = df.groupby(group_cols)
    groups_compute_args = [(Agg, agg_col, group) for group in groups]

    if parallel:
        with Pool() as pool:
            results = pool.starmap(compute_F_for_group, groups_compute_args)
    else:
        results = [compute_F_for_group(*group_args) for group_args in groups_compute_args]

    for group_key, agg_result, agg in results:
        output[group_key] = agg_result
        aggs[group_key] = agg
        keys.append(group_key)

    # Next, compute the solution (main DP).
    for key in keys:
        print(f"merging + pruning group: {key}")
        H = update_H(output[key], H, key)
        H = prune_H(H)

    ids_to_keep = []
    # TODO why is this enough? why do we not need to search for the best solution in H? Is it because of the pruning?
    agg_values_and_group_keys = H[H.keys()[-1]][1]
    for agg_value, group_key in agg_values_and_group_keys:
        ids_to_keep.extend(aggs[group_key].get_subset_for_value(agg_value))

    subset_df = df.iloc[ids_to_keep]
    removed_df = df.loc[~df.index.isin(ids_to_keep)]
    print("agg result after repair:")
    print(subset_df.groupby(group_cols)[agg_col].agg(['sum', 'count', 'mean', 'median']))
    #print(f"num_removed: {len(removed_df)}")

    return subset_df, removed_df

    # for agg_value, key in H[H.keys()[-1]][1]:
    #     items_in_key = list(get_subset_with_sum(vals[key], data[key], agg_value))
    #     for item in items_in_key:
    #         print((key[0], item[0]), item[1])
    #         needed_items[(key[0], item[0])] = item[1]
    #
    # indices_to_remove = []
    # for idx, row in df.iterrows():
    #     if ((row[group_cols[0]], row[agg_col]) in needed_items) and needed_items[
    #         (row[group_cols[0]], row[agg_col])] > 0:
    #         needed_items[(row[group_cols[0]], row[agg_col])] = needed_items[(row[group_cols[0]], row[agg_col])] - 1
    #     else:
    #         indices_to_remove.append(idx)
    # print(indices_to_remove)
    # df2 = df.drop(indices_to_remove).reset_index(drop=True)
    # df2.to_csv('df2_output.csv', index=False)
    # print(H[H.keys()[-1]])
