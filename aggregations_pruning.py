from typing import Dict, Protocol

import pandas as pd

from aggregations import get_index_set


class AggregationPruningFunction(Protocol):
    def __call__(self, df: pd.DataFrame, col: str, min_subset_size: int = None) -> Dict[float, set[int]]:
        ...


def get_sum_subsets_pruning(df: pd.DataFrame, agg_col: str, min_subset_size: int = None) -> Dict[float, set[int]]:
    # Dynamic programming dictionary: key = sum, value = indices of subset with sum & maximal size
    sum_subsets = {df[agg_col].sum(): get_index_set(df)}

    for index, row in df.iterrows():
        value = row[agg_col]
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


def get_avg_subsets_pruning(df: pd.DataFrame, agg_col: str, min_subset_size: int = None) -> Dict[float, set[int]]:
    # Dynamic programming dictionary: sum_subsets[s][k] is a subset with sum s and size k, if such subset exists
    sum_subsets = {df[agg_col].sum(): {len(df): get_index_set(df)}}

    for index, row in df.iterrows():
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
