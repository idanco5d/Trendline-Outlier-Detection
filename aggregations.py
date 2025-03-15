from itertools import combinations
from typing import Dict, Protocol

import numpy as np
import pandas as pd
from tqdm import tqdm

ERROR_EPSILON = 0.00001


def get_index_set(df: pd.DataFrame) -> set:
    return set(df.index)


class AggregationFunction(Protocol):
    def __call__(self, df: pd.DataFrame, col: str) -> Dict[float, set[int]]:
        ...


def get_max_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set[int]]:
    return {
        value: get_index_set(df.loc[df[agg_col].le(value)])
        for value in df[agg_col].unique()
    }


def get_min_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set[int]]:
    return {
        value: get_index_set(df.loc[df[agg_col].ge(value)])
        for value in df[agg_col].unique()
    }


def get_count_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set[int]]:
    return {
        count: get_index_set(df.head(count))
        for count in range(1, len(df) + 1)
    }


def get_count_distinct_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set[int]]:
    num_values = df[agg_col].nunique()
    value_counts = df[agg_col].value_counts()
    subsets = {}
    for count_limit in range(num_values + 1):
        common_values = value_counts.nlargest(count_limit).index
        subsets[count_limit] = get_index_set(df.loc[df[agg_col].isin(common_values)])
    return subsets


def get_sum_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set[int]]:
    # Dynamic programming dictionary: key = sum, value = indices of subset with sum & maximal size
    sum_subsets = {0: set()}  # Initialize with sum 0 having 0 rows and an empty subset

    for index, row in tqdm(df.iterrows(), total=len(df)):
        value = row[agg_col]
        current_sum_subsets = sum_subsets.copy()  # Avoid modifying dict while iterating

        for current_sum, subset in sum_subsets.items():
            new_sum = current_sum + value
            if new_sum not in current_sum_subsets or len(sum_subsets[new_sum]) < len(subset) + 1:
                current_sum_subsets[new_sum] = subset | {index}

        sum_subsets = current_sum_subsets

    return sum_subsets


def get_avg_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set[int]]:
    # Dynamic programming dictionary: sum_subsets[s][k] is a subset with sum s and size k, if such subset exists
    sum_subsets = {0: {0: set()}}

    for index, row in tqdm(df.iterrows(), total=len(df)):
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
    print("processing subset sums dict")
    avg_subsets: Dict[float, set] = {}
    for current_sum in tqdm(sum_subsets):
        for size in sum_subsets[current_sum]:
            subset = sum_subsets[current_sum][size]
            avg = 0 if size == 0 else current_sum / size
            if avg not in avg_subsets or len(avg_subsets[avg]) < len(subset):
                avg_subsets[avg] = subset

    return avg_subsets


def _get_median_subset_odd(df: pd.DataFrame, agg_col: str, median: float) -> set[int]:
    smaller_df = df.loc[df[agg_col].lt(median)]
    equal_df = df.loc[df[agg_col].eq(median)]
    greater_df = df.loc[df[agg_col].gt(median)]
    all_median = df[agg_col].median()

    if all_median > median:
        subset_df = pd.concat([
            smaller_df,
            equal_df,
            greater_df.head(len(smaller_df) + len(equal_df) - 1)
        ])

    elif all_median < median:
        subset_df = pd.concat([
            smaller_df.tail(len(equal_df) + len(greater_df) - 1),
            equal_df,
            greater_df
        ])

    else:
        subset_df = df

    if np.abs(subset_df[agg_col].median() - median) > ERROR_EPSILON:
        raise Exception('Reached wrong median')

    return get_index_set(subset_df)


def _get_median_subset_even(df: pd.DataFrame, agg_col: str, low: float, high: float) -> set[int]:
    median = (low + high) / 2
    smaller_df = df.loc[df[agg_col].le(low)]
    greater_df = df.loc[df[agg_col].ge(high)]

    if len(smaller_df) < len(greater_df):
        subset_df = pd.concat([smaller_df, greater_df.head(len(smaller_df))])
    elif len(smaller_df) > len(greater_df):
        subset_df = pd.concat([smaller_df.tail(len(greater_df)), greater_df])
    else:
        subset_df = pd.concat([smaller_df, greater_df])

    if np.abs(subset_df[agg_col].median() - median) > ERROR_EPSILON:
        print(subset_df[agg_col].values)
        print(len(subset_df))
        print(subset_df[agg_col].median())
        print(median)
        raise Exception('Reached wrong median')

    return get_index_set(subset_df)


def get_median_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set[int]]:
    median_subsets = {}
    df = df.sort_values(by=agg_col)
    unique_values = df[agg_col].unique()

    for value in tqdm(unique_values):
        median_subsets[value] = _get_median_subset_odd(df, agg_col, value)

    for low, high in tqdm(combinations(unique_values, 2)):
        median = (low + high) / 2
        subset = _get_median_subset_even(df, agg_col, low, high)
        if median not in median_subsets or len(median_subsets[median]) < len(subset):
            median_subsets[median] = subset

    return median_subsets
