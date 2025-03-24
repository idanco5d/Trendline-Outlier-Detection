from itertools import combinations
from typing import Dict, Protocol
from collections import defaultdict

import numpy as np
import pandas as pd
from tqdm import tqdm

ERROR_EPSILON = 0.00001


class AggregationMem(object):
    def __init__(self):
        pass
        
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str) -> Dict[float, int]:
        pass
    
    def get_subset_for_value(self, df: pd.DataFrame, agg_col: str, required_value: float):
        pass




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

class MaxAggregation(AggregationMem):
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str) -> Dict[float, int]:
        hist = df[agg_col].value_counts().sort_index().cumsum().to_dict()
        return hist
        
    def get_subset_for_value(self, df: pd.DataFrame, agg_col: str, required_value: float):
        return get_index_set(df.loc[df[agg_col].le(required_value)])


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



class SumAggregation(AggregationMem):
    def __init__(self):
        self.tuples = None
        self.subset_sizes = None
    
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str) -> Dict[float, int]:
        inf = len(df)+1
        self.tuples = list(df[agg_col].to_dict().items()) # tuples of index and agg_col value
        
        first_value = self.tuples[0][1]
        # subset_sizes is a dict of the form {j: {s: subset size}}, j - the index of the tuple in an ordered list, s - the sum.
        subset_sizes = defaultdict(lambda: defaultdict(lambda: -inf))
        subset_sizes[0][0] = 0 # initialize with an empty size (size 0) having sum 0
        subset_sizes[0][first_value] = 1  # Initialize with sum first_value having the first tuple (subset size 1)

        for j in range(1, len(self.tuples)):
            value = self.tuples[j][1] # value of the current tuple
            current_subset_sizes = defaultdict(lambda: -inf)  # Avoid modifying dict while iterating
            
            for current_sum, subset_size in subset_sizes[j-1].items():
                if current_sum not in current_subset_sizes or current_subset_sizes[current_sum] < subset_size:
                    # without the current tuple
                    current_subset_sizes[current_sum] = subset_size
                if current_sum + value not in current_subset_sizes or current_subset_sizes[current_sum + value] < subset_size + 1:
                    # with the current tuple
                    current_subset_sizes[current_sum + value] = subset_size + 1
            subset_sizes[j] = current_subset_sizes
        self.subset_sizes = subset_sizes
        # create a dict of agg_val to max_subset_size:
        val_to_max_size = subset_sizes[len(self.tuples)-1]
        return val_to_max_size
        
    def get_subset_for_value(self, required_value: float):
        if self.subset_sizes is None:
            raise Exception("Subset sizes empty when get_subset_for_value was called")
        j = len(self.tuples)-1
        # for sanity check - the expected size
        required_size = self.subset_sizes[j][required_value]
        s = required_value
        subset = []
        for j in range(len(self.tuples)-1, -1, -1): # j=n,...,0
            tuple_j_value = self.tuples[j][1]
            if j == 0:
                if tuple_j_value == s:
                    subset.append(self.tuples[j][0])
                    break
            else:
                without_j = self.subset_sizes[j-1][s]
                with_j = self.subset_sizes[j-1][s-tuple_j_value] + 1
                if with_j >= without_j:
                    subset.append(self.tuples[j][0])
                    s -= tuple_j_value
        if len(subset) != required_size:
            print(subset2)
            print(len(subset))
            print(required_size)
            raise Exception("returned subset size does not match DP table")
        if len(subset) != len(set(subset)):
            raise Exception("return subset has duplicate indices")
        return subset
        



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
                    continue #why would index ever be in subset??
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

   
    
def _get_median_subset_even(tuples: list[tuple], low_index: float, high_index: float) -> set[int]:
    
    median = (tuples[low_index][1] + tuples[high_index][1])/2
    N = len(tuples)

    if low_index < N - high_index - 1:
        subset = tuples[:low_index+1] + tuples[high_index: high_index+low_index+1]
    elif low_index > N - high_index - 1:
        subset = tuples[low_index+high_index-N+1 : low_index+1] + tuples[high_index:]
    else:
        subset = tuples[:low_index + 1] + tuples[high_index:]

    new_median = np.median([x[1] for x in subset])
    if np.abs(new_median - median) > ERROR_EPSILON:
        print(tuples)
        print(low_index, high_index)
        print(tuples[low_index], tuples[high_index])
        print(subset)
        print(new_median)
        print(median)
        raise Exception('Reached wrong median')

    return set([x[0] for x in subset])


def get_median_subsets(df: pd.DataFrame, agg_col: str) -> Dict[float, set[int]]:
    median_subsets = {}
    df = df.sort_values(by=agg_col)
    unique_values = df[agg_col].unique()

    for value in tqdm(unique_values):
        median_subsets[value] = _get_median_subset_odd(df, agg_col, value)

    tuples = list(df[agg_col].to_dict().items()) # tuples of index and agg_col value
    for low_index in tqdm(range(len(tuples))):
        for high_index in range(low_index + 1, len(tuples)): # here we would prune
            median = (tuples[low_index][1] + tuples[high_index][1])/2
            subset = _get_median_subset_even(tuples, low_index, high_index)
            if median not in median_subsets or len(median_subsets[median]) < len(subset):
                median_subsets[median] = subset
    return median_subsets

