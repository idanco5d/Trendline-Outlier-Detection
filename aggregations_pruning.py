from multiprocessing import cpu_count, Pool
from typing import Dict, Protocol

import pandas as pd

from aggregations import get_index_set, _get_median_subset_even, _get_median_subset_odd
from tqdm import tqdm


class AggregationPruningFunction(Protocol):
    def __call__(self, df: pd.DataFrame, col: str, min_subset_size: int = None, parallelize: bool = False) -> Dict[float, set[int]]:
        ...


def get_sum_subsets_pruning(df: pd.DataFrame, agg_col: str, min_subset_size: int = None, parallelize: bool = False) -> Dict[float, set[int]]:
    # Dynamic programming dictionary: key = sum, value = indices of subset with sum & maximal size
    # TODO: save the indices to remove instead of the indices to keep
    sum_subsets = {df[agg_col].sum(): get_index_set(df)}

    for index, row in tqdm(df.iterrows(), total=len(df)):
        value = row[agg_col]
        # TODO: maybe avoid this - too many copies
        current_sum_subsets = sum_subsets.copy()  # Avoid modifying dict while iterating

        for current_sum, subset in sum_subsets.items():
            if min_subset_size is not None and len(subset) <= min_subset_size:
                continue
            if index not in subset:
                continue
            new_sum = current_sum - value
            if new_sum not in current_sum_subsets or len(sum_subsets[new_sum]) < len(subset) - 1:
                current_sum_subsets[new_sum] = subset - {index}

        sum_subsets = current_sum_subsets

    return sum_subsets


def get_avg_subsets_pruning(df: pd.DataFrame, agg_col: str, min_subset_size: int = None, parallelize: bool = False) -> Dict[float, set[int]]:
    # Dynamic programming dictionary: sum_subsets[s][k] is a subset with sum s and size k, if such subset exists
    sum_subsets = {df[agg_col].sum(): {len(df): get_index_set(df)}}

    for index, row in tqdm(df.iterrows(), total=len(df)):
        value = row[agg_col]
        # keep a static copy of the keys because we are adding keys in the loop
        for current_sum in list(sum_subsets.keys()):
            for size in list(sum_subsets[current_sum].keys()):
                if min_subset_size is not None and size <= min_subset_size:
                    continue
                subset = sum_subsets[current_sum][size]
                if index not in subset:
                    continue
                new_sum = current_sum - value
                new_size = size - 1
                if new_sum not in sum_subsets:
                    sum_subsets[new_sum] = {}
                if new_size not in sum_subsets[new_sum]:
                    sum_subsets[new_sum][new_size] = subset - {index}

    avg_subsets: Dict[float, set] = {}
    for current_sum in sum_subsets:
        for size in sum_subsets[current_sum]:
            subset = sum_subsets[current_sum][size]
            avg = 0 if size == 0 else current_sum / size
            if avg not in avg_subsets or len(avg_subsets[avg]) < len(subset):
                avg_subsets[avg] = subset

    return avg_subsets


shared_tuples = None

def init_worker(tuples_):
    global shared_tuples
    shared_tuples = tuples_

def process_low_index(args):
    global shared_tuples
    low_index, min_subset_size = args
    medians_and_subsets = []
    if min_subset_size is None:
        max_distance = len(shared_tuples) - low_index
    else:
        max_removed = len(shared_tuples) - min_subset_size
        max_distance = min(len(shared_tuples) - low_index, max_removed + 1)
    for high_index in range(low_index + 1,
                            low_index + max_distance):  # here we prune: don't consider indices which are too far apart
        median = (shared_tuples[low_index][1] + shared_tuples[high_index][1]) / 2
        subset = _get_median_subset_even(shared_tuples, low_index, high_index)
        medians_and_subsets.append((median, subset))
    return medians_and_subsets


def get_median_subsets_pruning(df: pd.DataFrame, agg_col: str, min_subset_size: int = None,
                               parallelize: bool = False) -> Dict[float, set[int]]:
    median_subsets = {}
    df = df.sort_values(by=agg_col)
    unique_values = df[agg_col].unique()

    for value in tqdm(unique_values):
        median_subsets[value] = _get_median_subset_odd(df, agg_col, value)
    tuples = list(df[agg_col].to_dict().items()) # tuples of index and agg_col value

    if parallelize:
        pool_args = [(low_index, min_subset_size) for low_index in range(len(tuples))]
        max_workers = min(20, cpu_count())
        with Pool(processes=max_workers, initializer=init_worker, initargs=(tuples,)) as pool:
            results = list(tqdm(pool.imap(process_low_index, pool_args), total=len(pool_args)))
    
        for medians_and_subsets in results:
            if medians_and_subsets is not None:
                for med, sub in medians_and_subsets:
                    if med not in median_subsets or len(median_subsets[med]) < len(sub):
                        median_subsets[med] = sub
    else:  # serial execution
        for low_index in tqdm(range(len(tuples))):
            if min_subset_size is None:
                max_distance = len(tuples) - low_index
            else:
                max_removed = len(tuples) - min_subset_size
                max_distance = min(len(tuples) - low_index, max_removed+1)
            for high_index in range(low_index + 1, low_index + max_distance): # here we prune: don't consider indices which are too far apart
                #print(high_index, max_removed)
                median = (tuples[low_index][1] + tuples[high_index][1])/2
                subset = _get_median_subset_even(tuples, low_index, high_index)
                if median not in median_subsets or len(median_subsets[median]) < len(subset):
                    median_subsets[median] = subset
    return median_subsets
