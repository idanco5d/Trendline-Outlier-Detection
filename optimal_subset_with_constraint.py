from typing import Dict, List, Union, Type
from sortedcontainers import SortedDict
import sys
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
#import concurrent.futures

from aggregations import AggregationFunction
from aggregations_mem import AggregationMem
from constants import *

_shared_value_subsets = None

def init_worker(value_subsets_data):
    global _shared_value_subsets
    _shared_value_subsets = value_subsets_data


def get_optimal_subset(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        agg: AggregationFunction
) -> (pd.DataFrame, pd.DataFrame):
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    # Dynamic programming table: key = agg value, value = maximal subset with agg value
    value_subsets: SortedDict[float, set] = SortedDict()
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

        # More pruning!
        # If we have v1, v2 s.t. v1<=v2 and size(value_subsets[v1])>=size(value_subsets[v2]), no need to save v2.
        # We would always prefer the solution for v1.
        max_keep_size = 0
        pruned_value_subsets = {}
        for x in sorted(new_value_subsets.keys()):
            subset = new_value_subsets[x]
            keep_size = len(subset)
            if keep_size > max_keep_size:
                pruned_value_subsets[x] = subset
                max_keep_size = keep_size

        value_subsets = SortedDict(pruned_value_subsets)

        #value_subsets = new_value_subsets

    optimal_subset = list(get_maximal_set_with_upper_bound(value_subsets))
    subset_df = df.iloc[optimal_subset]
    removed_df = df.loc[~df.index.isin(optimal_subset)]

    return subset_df, removed_df


def get_maximal_set_with_upper_bound(value_sets: SortedDict[float, set], upper_bound: float = None) -> set:
    maximal_set = set()
    if upper_bound is not None:
        index = value_sets.bisect_left(upper_bound)
        # should include keys[index]? yes, if it is still <= upper bound (i.e., equal to it)
        if index < len(value_sets) and value_sets.keys()[index] <= upper_bound:
            index += 1
        values_to_consider = value_sets.keys()[:index]
    else:
        values_to_consider = value_sets.keys()
    for current_value in values_to_consider:
        current_set = value_sets[current_value]
        if len(current_set) > len(maximal_set):
            maximal_set = current_set
    return maximal_set


def get_optimal_subset_mem_opt(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        Agg: AggregationMem,
        time_cutoff_seconds: int = None,
        # TODO: parallelize the long loop!
) -> (pd.DataFrame, pd.DataFrame):
    print("mem opt")
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    print("agg result before removal:")
    print(df.groupby(group_cols)[agg_col].sum())
    # Dynamic programming table: key = agg value, value = maximal subset with agg value
    value_subsets: Dict[float, Dict[int, tuple]] = SortedDict()  # agg_value -> dict of group_id to (size, agg_val) (for the previous groups)
    group_to_agg = {}
    for group_key, group_df in df.groupby(group_cols):  # groupby keys are sorted by default
        print(f"working on group: {group_key}")
        agg = Agg()
        group_to_agg[group_key] = agg  # save it for later
        current_group_subsets = agg.compute_max_subset_sizes(group_df, agg_col)  # dict of agg_val: maximal subset size
        new_value_subsets: Dict[float, Dict[int, tuple]] = {}  # agg_val of current ri -> {group_id -> (size, agg_val)}

        iterations_over_time_limit = 0
        with tqdm(current_group_subsets.items()) as t:
            for value, subset_size in t:
                d = t.format_dict
                if d['rate'] is not None:
                    remaining_time_estimate = (d['total'] - d['n'])/d['rate']
                    if time_cutoff_seconds is not None and remaining_time_estimate > time_cutoff_seconds:
                        iterations_over_time_limit += 1
                if iterations_over_time_limit > 5000:
                    print(d)
                    print(f"estimated time left is too high: {remaining_time_estimate/60} minutes, exiting")
                    sys.exit()

        #for value, subset_size in current_group_subsets.items():
                previous_groups_subset_sizes_and_values = get_maximal_set_sizes_with_upper_bound(value_subsets, value)
                previous_groups_subset_sizes_and_values[group_key] = (subset_size, value)
                new_value_subsets[value] = previous_groups_subset_sizes_and_values

        for value in value_subsets.keys():
            if value not in new_value_subsets.keys():
                new_value_subsets[value] = value_subsets[value]

        # More pruning!
        # If we have v1, v2 s.t. v1<=v2 and size(value_subsets[v1])>=size(value_subsets[v2]), no need to save v2.
        # We would always prefer the solution for v1.
        max_keep_size = 0
        pruned_value_subsets = {}
        for x in sorted(new_value_subsets.keys()):
            groups_subset_sizes_and_values = new_value_subsets[x]
            keep_size = sum([groups_subset_sizes_and_values[gid][0] for gid in groups_subset_sizes_and_values])
            if keep_size > max_keep_size:
                pruned_value_subsets[x] = groups_subset_sizes_and_values
                max_keep_size = keep_size

        value_subsets = SortedDict(pruned_value_subsets)

        #value_subsets = new_value_subsets.copy()

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


def get_maximal_set_sizes_with_upper_bound(value_sets: SortedDict[float, Dict[int, tuple]], upper_bound: float = None) -> Dict[int, tuple]:
    # value_sets: dict of {agg_value : {group_id: (size, agg_val)}}
    if upper_bound is not None:
        index = value_sets.bisect_left(upper_bound)
        # should include keys[index]? yes, if it is still <= upper bound (i.e., equal to it)
        if index < len(value_sets) and value_sets.keys()[index] <= upper_bound:
            index += 1
        values_to_consider = value_sets.keys()[:index]
    else:
        values_to_consider = value_sets.keys()
    max_repair_size = 0
    repair_sizes_and_values = {}
    for current_value in values_to_consider:
        gid_to_size_and_val = value_sets[current_value]
        current_size = sum([gid_to_size_and_val[group_id][0] for group_id in gid_to_size_and_val])
        if current_size > max_repair_size:
            max_repair_size = sum([gid_to_size_and_val[group_id][0] for group_id in gid_to_size_and_val])
            repair_sizes_and_values = gid_to_size_and_val.copy()
    return repair_sizes_and_values


def process_subset_item(args):
    value, subset_size, group_key, group_size, group_to_orig_size, max_removed = args

    global _shared_value_subsets
    value_subsets = _shared_value_subsets

    previous_groups_subset_sizes_and_values = get_maximal_set_sizes_with_upper_bound(value_subsets, value)

    if not previous_groups_subset_sizes_and_values:
        previous_removed_count = 0
    else:
        previous_removed_count = sum(
            group_to_orig_size[gid] - previous_groups_subset_sizes_and_values[gid][0]
            for gid in previous_groups_subset_sizes_and_values
        )

    new_removed_count = previous_removed_count + group_size - subset_size

    if new_removed_count <= max_removed:
        previous_groups_subset_sizes_and_values[group_key] = (subset_size, value)
        return value, previous_groups_subset_sizes_and_values
    return None


def get_optimal_subset_pruning_mem_opt(
        df: pd.DataFrame,
        group_cols: Union[str, List[str]],
        agg_col: str,
        Agg: Type[AggregationMem],
        max_removed: int = None,
        time_cutoff_seconds: int = None,
        parallelize: bool = False,
) -> (pd.DataFrame, pd.DataFrame):
    print("mem opt + pruning")
    df = df.loc[df[group_cols].notnull().all(axis=1)].reset_index(drop=True)
    print("agg result before removal:")
    print(df.groupby(group_cols)[agg_col].mean())
    # Dynamic programming table:
    # key = agg value
    # Value = maximal subset with agg value & number of removed tuples
    #value_subsets: Dict[float, Tuple[set, int]] = {}

    # Dynamic programming table: key = agg value, value = maximal subset with agg value
    value_subsets: Dict[float, Dict[int, tuple]] = SortedDict() # agg_value -> dict of group_id to (size, agg_val) (for the previous groups)
    group_to_agg = {}
    group_to_orig_size = {}
    for group_key, group_df in df.groupby(group_cols):  # groupby keys are sorted by default
        group_to_orig_size[group_key] = len(group_df)
        print(f"working on group: {group_key}")
        min_subset_size = len(group_df) - max_removed if max_removed is not None else None
        agg = Agg(parallelize)
        group_to_agg[group_key] = agg  # save it for later
        current_group_subsets = agg.compute_max_subset_sizes(group_df, agg_col, min_subset_size)  # dict of agg_val: maximal subset size
        
        print(f"current_group_subsets len: {len(current_group_subsets)} and size: {sys.getsizeof(current_group_subsets)}")
        print(f"size of agg: {sys.getsizeof(agg.subset_sizes)}")
        new_value_subsets: Dict[float, Dict[int, tuple]] = {}  # agg_val of current ri -> {group_id -> (size, agg_val)}

        if parallelize:
            pool_args = [(value, subset_size, group_key, len(group_df), group_to_orig_size, max_removed)
                          for value, subset_size in current_group_subsets.items()]
            num_workers = min(NUM_PROCESSES, cpu_count())
            with Pool(processes=num_workers, initializer=init_worker, initargs=(value_subsets,)) as pool:
                results = list(tqdm(pool.imap(process_subset_item, pool_args), total=len(pool_args)))

            for result in results:
                if result is not None:
                    value, subset_map = result
                    new_value_subsets[value] = subset_map
            
        else:
            print("Not parallelizing")
            iterations_over_time_limit = 0
            with tqdm(current_group_subsets.items()) as t:
                for value, subset_size in t:
                    d = t.format_dict
                    if d['rate'] is not None:
                        remaining_time_estimate = (d['total'] - d['n'])/d['rate']
                        if time_cutoff_seconds is not None and remaining_time_estimate > time_cutoff_seconds:
                            iterations_over_time_limit += 1
                    if iterations_over_time_limit > 5000:
                        print(d)
                        print(f"estimated time left is too high: {remaining_time_estimate/60} minutes, exiting")
                        sys.exit()
                    previous_groups_subset_sizes_and_values = get_maximal_set_sizes_with_upper_bound(
                        value_subsets, value
                    )
                    if len(previous_groups_subset_sizes_and_values) == 0:
                        previous_removed_count = 0
                    else:
                        previous_removed_count = sum([group_to_orig_size[gid] - previous_groups_subset_sizes_and_values[gid][0]
                                                      for gid in previous_groups_subset_sizes_and_values])
                    new_removed_count = previous_removed_count + len(group_df) - subset_size
                    if new_removed_count <= max_removed:
                        previous_groups_subset_sizes_and_values[group_key] = (subset_size, value)
                        new_value_subsets[value] = previous_groups_subset_sizes_and_values

        for value in value_subsets.keys():
            if value not in new_value_subsets.keys():
                new_value_subsets[value] = value_subsets[value]

        # More pruning!
        # If we have v1, v2 s.t. v1<=v2 and size(value_subsets[v1])>=size(value_subsets[v2]), no need to save v2.
        # We would always prefer the solution for v1.
        max_keep_size = 0
        pruned_value_subsets = {}
        for x in sorted(new_value_subsets.keys()):
            groups_subset_sizes_and_values = new_value_subsets[x]
            keep_size = sum([groups_subset_sizes_and_values[gid][0] for gid in groups_subset_sizes_and_values])
            if keep_size > max_keep_size:
                pruned_value_subsets[x] = groups_subset_sizes_and_values
                max_keep_size = keep_size

        value_subsets = SortedDict(pruned_value_subsets)
        #value_subsets = new_value_subsets
        print(f"len value_subsets: {len(value_subsets)} size of value subsets: {sys.getsizeof(value_subsets)}")
    gid_to_size_and_val = get_maximal_set_sizes_with_upper_bound(value_subsets)
    optimal_subset = []
    for gid in gid_to_size_and_val.keys():
        req_size, req_value = gid_to_size_and_val[gid]
        group_subset = group_to_agg[gid].get_subset_for_value(req_value)
        if len(group_subset) != req_size:
            print(f"group_subset size: {len(group_subset)}, required_size: {req_size}")
            raise Exception(f"mismatch in subset sizes for group: {gid}")
        optimal_subset.extend(group_subset)
    subset_df = df.iloc[optimal_subset]
    removed_df = df.loc[~df.index.isin(optimal_subset)]
    print("agg result after removal:")
    print(subset_df.groupby(group_cols)[agg_col].mean())

    return subset_df, removed_df

