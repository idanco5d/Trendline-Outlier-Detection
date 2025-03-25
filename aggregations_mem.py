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



class MaxAggregation(AggregationMem):
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str) -> Dict[float, int]:
        hist = df[agg_col].value_counts().sort_index().cumsum().to_dict()
        return hist
        
    def get_subset_for_value(self, df: pd.DataFrame, agg_col: str, required_value: float):
        return get_index_set(df.loc[df[agg_col].le(required_value)])





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
        


class AvgAggregation(AggregationMem):
    def __init__(self):
        self.tuples = None
        self.subset_sizes = None
    
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str) -> Dict[float, int]:
        inf = len(df)+1
        self.tuples = list(df[agg_col].to_dict().items()) # tuples of index and agg_col value

        first_value = self.tuples[0][1]
        # subset_sizes is a dict of the form {j: {s: {k: subset size}}}, 
        # where j - the index of the tuple in an ordered list, s - the sum, and k - the size. subset_size is either k or -inf if no such subset exists.
        subset_sizes = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: -inf)))
        subset_sizes[0][0][0] = 0 # initialize with an empty size (size 0) having sum 0
        subset_sizes[0][first_value][1] = 1  # Initialize with sum first_value having the first tuple (subset size 1)

        for j in range(1, len(self.tuples)):
            value = self.tuples[j][1] # value of the current tuple
            current_subset_sizes = defaultdict(lambda: defaultdict(lambda: -inf))  # Avoid modifying dict while iterating

            for current_sum in subset_sizes[j-1].keys():
                for current_size in subset_sizes[j-1][current_sum]:
                    if current_sum not in current_subset_sizes or current_size not in current_subset_sizes[current_sum]:
                        # without the current tuple
                        current_subset_sizes[current_sum][current_size] = current_size
                    if current_sum + value not in current_subset_sizes or (current_size+1) not in current_subset_sizes[current_sum + value]:
                        # with the current tuple
                        current_subset_sizes[current_sum + value][current_size + 1] = current_size + 1
            subset_sizes[j] = current_subset_sizes
        self.subset_sizes = subset_sizes
        avg_subsets: Dict[float, tuple] = {} # avg value to (size, sum)
        for current_sum in subset_sizes[len(self.tuples)-1]:
            for size in subset_sizes[len(self.tuples)-1][current_sum]:
                avg = 0 if size == 0 else current_sum / size
                if avg not in avg_subsets or avg_subsets[avg] < size:
                    avg_subsets[avg] = size
        return avg_subsets

    def get_subset_for_value(self, required_value: float):
        if self.subset_sizes is None:
            raise Exception("Subset sizes empty when get_subset_for_value was called")
        j = len(self.tuples)-1
        # get sum and size for the required_value
        best_sum, best_size = 0, 0
        for current_sum in self.subset_sizes[len(self.tuples)-1]:
            for size in self.subset_sizes[len(self.tuples)-1][current_sum]:
                avg = 0 if size == 0 else current_sum / size
                if np.abs(avg - required_value) <= ERROR_EPSILON:
                    if size > best_size:
                        best_sum, best_size = current_sum, size
        
        # for sanity check - the expected size
        required_size = self.subset_sizes[j][best_sum][best_size]
        if required_size != best_size:
            print("avg: mismatch in subset sizes")
        s = best_sum
        c = best_size # this is exactly how many tuples we will keep
        subset = []
        for j in range(len(self.tuples)-1, -1, -1): # j=n,...,0
            tuple_j_value = self.tuples[j][1]
            if j == 0:
                if tuple_j_value == s:
                    subset.append(self.tuples[j][0])
                    break
            else:
                without_j = self.subset_sizes[j-1][s][c]
                with_j = self.subset_sizes[j-1][s-tuple_j_value][c-1] + 1
                if with_j >= without_j:
                    subset.append(self.tuples[j][0])
                    s -= tuple_j_value
                    c -= 1
        if len(subset) != required_size:
            print(subset2)
            print(len(subset))
            print(required_size)
            raise Exception("returned subset size does not match DP table")
        if len(subset) != len(set(subset)):
            raise Exception("return subset has duplicate indices")
        actual_avg = np.mean([t[1] for t in self.tuples if t in subset])
        if np.abs(actual_avg - required_value) > ERROR_EPSILON:
            print(f"Mismatch in avg values: actual: {actual_avg} required: {required_value}")
        return subset

