from typing import Dict

import pandas as pd


def get_index_set(df: pd.DataFrame) -> set:
    return set(df.index)


def get_max_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set]:
    return {
        value: get_index_set(df.loc[df[agg_col].le(value)])
        for value in df[agg_col].unique()
    }


def get_min_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set]:
    return {
        value: get_index_set(df.loc[df[agg_col].ge(value)])
        for value in df[agg_col].unique()
    }


def get_count_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set]:
    return {
        count: get_index_set(df.head(count))
        for count in range(1, len(df) + 1)
    }


def get_count_distinct_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set]:
    num_values = df[agg_col].nunique()
    value_counts = df[agg_col].value_counts()
    subsets = {}
    for count_limit in range(num_values + 1):
        common_values = value_counts.nlargest(count_limit).index
        subsets[count_limit] = get_index_set(df.loc[df[agg_col].isin(common_values)])
    return subsets


def get_sum_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set]:
    # Dynamic programming dictionary: key = sum, value = indices of subset with sum & maximal size
    sum_subsets = {0: set()}  # Initialize with sum 0 having 0 rows and an empty subset

    for index, row in df.iterrows():
        value = row[agg_col]
        current_sum_subsets = sum_subsets.copy()  # Avoid modifying dict while iterating

        for current_sum, subset in sum_subsets.items():
            new_sum = current_sum + value
            if new_sum not in current_sum_subsets or len(sum_subsets[new_sum]) < len(subset) + 1:
                current_sum_subsets[new_sum] = subset | {index}

        sum_subsets = current_sum_subsets

    return sum_subsets


def get_avg_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set]:
    # Dynamic programming dictionary: sum_subsets[s][k] is a subset with sum s and size k, if such subset exists
    sum_subsets = {0: {0: set()}}

    for index, row in df.iterrows():
        value = row[agg_col]
        # keep a static copy of the keys because we are adding keys in the loop
        for current_sum in list(sum_subsets.keys()):
            for size in list(sum_subsets[current_sum].keys()):
                subset = sum_subsets[current_sum][size]
                if index in subset:
                    continue
                new_sum = current_sum + value
                new_size = size + 1
                if new_sum not in sum_subsets:
                    sum_subsets[new_sum] = {}
                if new_size not in sum_subsets[new_sum]:
                    sum_subsets[new_sum][new_size] = subset | {index}

    avg_subsets: Dict[float, set] = {}
    for current_sum in sum_subsets:
        for size in sum_subsets[current_sum]:
            subset = sum_subsets[current_sum][size]
            avg = 0 if size == 0 else current_sum / size
            if avg not in avg_subsets or len(avg_subsets[avg]) < len(subset):
                avg_subsets[avg] = subset

    return avg_subsets
