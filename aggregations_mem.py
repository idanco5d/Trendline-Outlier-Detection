import sys
from itertools import combinations
from typing import Dict, Protocol
from collections import defaultdict
from multiprocessing import Pool, cpu_count

from collections import defaultdict, Counter
import math

import numpy as np
import pandas as pd
from tqdm import tqdm

from constants import *
# ERROR_EPSILON = 0.00001
# NUM_PROCESSES = 20


class AggregationMem(object):
    def __init__(self, parallelize=False):
        pass
        
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds: int = None) -> Dict[float, int]:
        pass
    
    def get_subset_for_value(self, required_value: float):
        pass




def get_index_set(df: pd.DataFrame) -> set:
    return set(df.index)


class AggregationFunction(Protocol):
    def __call__(self, df: pd.DataFrame, col: str) -> Dict[float, set[int]]:
        ...


class MaxAggregation(AggregationMem):
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds=None) -> Dict[float, int]:
        hist = df[agg_col].value_counts().sort_index().cumsum().to_dict()
        self.df = df
        self.agg_col = agg_col
        return hist
        
    def get_subset_for_value(self, required_value: float):
        return get_index_set(self.df.loc[self.df[self.agg_col].le(required_value)])


class SumAggregation(AggregationMem):
    def __init__(self, parallelize=False):
        self.tuples = None
        self.subset_sizes = None
    
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds=None) -> Dict[float, int]:
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
            print(len(subset))
            print(required_size)
            raise Exception("returned subset size does not match DP table")
        if len(subset) != len(set(subset)):
            raise Exception("return subset has duplicate indices")
        return subset


class SumAggregationPruning(AggregationMem):
    def __init__(self, parallelize=False):
        self.tuples = None
        self.subset_sizes = None

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds=None) -> Dict[float, int]:
        self.df = df
        self.total_sum = self.df[agg_col].sum()
        inf = len(df) + 1
        self.tuples = list(df[agg_col].to_dict().items())  # tuples of index and agg_col value

        first_value = self.tuples[0][1]
        # subset_sizes is a dict of the form {j: {s: subset size}},
        # j - the index of the tuple in an ordered list, s - the sum *of the removed set*.
        subset_sizes = defaultdict(lambda: defaultdict(lambda: -inf))
        subset_sizes[0][0] = 0  # initialize with an empty size (size 0) having sum 0
        subset_sizes[0][first_value] = 1  # Initialize with sum first_value having the first tuple (subset size 1)

        for j in range(1, len(self.tuples)):
            value = self.tuples[j][1]  # value of the current tuple
            current_subset_sizes = defaultdict(lambda: -inf)  # Avoid modifying dict while iterating

            for current_sum, subset_size in subset_sizes[j - 1].items():
                # pruning by a given bound on the solution size
                if max_removed is not None and subset_size > max_removed:
                    continue
                if current_sum not in current_subset_sizes or current_subset_sizes[current_sum] < subset_size:
                    # without the current tuple
                    current_subset_sizes[current_sum] = subset_size
                if current_sum + value not in current_subset_sizes or current_subset_sizes[
                    current_sum + value] < subset_size + 1:
                    # with the current tuple
                    current_subset_sizes[current_sum + value] = subset_size + 1
            subset_sizes[j] = current_subset_sizes
        self.subset_sizes = subset_sizes
        # create a dict of agg_val to max_subset_size:
        val_to_max_size = {self.total_sum - s: size for s, size in subset_sizes[len(self.tuples) - 1].items()}
        return val_to_max_size

    def get_subset_for_value(self, required_value: float):
        required_removed_sum = self.total_sum - required_value
        if self.subset_sizes is None:
            raise Exception("Subset sizes empty when get_subset_for_value was called")
        j = len(self.tuples) - 1
        # for sanity check - the expected size
        required_size = self.subset_sizes[j][required_removed_sum]
        s = required_removed_sum
        subset = []
        for j in range(len(self.tuples) - 1, -1, -1):  # j=n,...,0
            tuple_j_value = self.tuples[j][1]
            if j == 0:
                if tuple_j_value == s:
                    subset.append(self.tuples[j][0])
                    break
            else:
                without_j = self.subset_sizes[j - 1][s]
                with_j = self.subset_sizes[j - 1][s - tuple_j_value] + 1
                if with_j >= without_j:
                    subset.append(self.tuples[j][0])
                    s -= tuple_j_value
        if len(subset) != required_size:
            print(len(subset))
            print(required_size)
            raise Exception("returned subset size does not match DP table")
        if len(subset) != len(set(subset)):
            raise Exception("return subset has duplicate indices")
        return subset



class SumAggregationOpt(AggregationMem):
    def __init__(self, parallelize=False):
        self.tuples = None
        self.subset_sizes = None

    def calc_knapsack(self, items, hist):
        #items = sorted(df[agg_col].values)  # this part is important to ensure there are no duplicates

        # make a histogram
        #hist = list(df[agg_col].value_counts().items())

        # Cell s in sum_to_max_size will contain the maximal size of a subset with sum s.
        # sum_to_max_size[s] = maximal size of a subset with sum s
        sum_to_max_size = [-1] * (sum(items) + 1)
        # data is a matrix. data[j][s] = maximal size of subset using values v1,..,vj with sum s (or -1 if there is not such subset)
        data = []
        sum_to_max_size[0] = 0
        maximal_sum_reached = 0  # maximal sum possible using unique values v1,..,vj (vj - current vj)

        for vj, amt in tqdm(hist):
            # print(vj, maximal_sum_reached)
            # current_arr is the next row in data (for the unique values up to vj, and all the possible sums.)
            # current_arr[s] = how many items of value vj were used to reach sum s in the optimal solution.
            current_arr = [0] * (sum(items) + 1)
            maximal_sum_reached += vj * amt
            if vj == 0:
                current_arr[0] = amt
                data.append(current_arr)
                sum_to_max_size[0] = amt
                continue
            temp_sum_to_max_size = sum_to_max_size.copy()
            # Go over all possible sums that can be reached with items of value <vj>.
            # In each iteration, we attempt to add a vj item to the solution.
            for s in range(vj, maximal_sum_reached + 1):
                # how many items with value vj are included in the optimal solution for sum s-vj
                num_used_vj_items = current_arr[s - vj]
                sum_without_vj_items = s - (num_used_vj_items + 1) * vj
                # if an item could be included but it doesn't increase the number of items - we don't take it
                if (num_used_vj_items < amt  # there are unused items of value vj
                        and sum_without_vj_items >= 0  # TODO: possibly redundant
                        # and using including n vj items is better than the current optimal solution without vj items
                        and sum_to_max_size[sum_without_vj_items] + num_used_vj_items + 1 > sum_to_max_size[s]):
                    # add another vj item
                    current_arr[s] = num_used_vj_items + 1
                    temp_sum_to_max_size[s] = sum_to_max_size[sum_without_vj_items] + num_used_vj_items + 1

                if num_used_vj_items == amt:  # all vj items are used
                    for num_used in range(1, amt + 1):
                        # use as little as possible vj items
                        # temp_sum_to_max_size[s] - current size of optimal solution for sum i (using less than num_used items with "vj")
                        # sum_to_max_size[s - vj * num_used] + num_used - size of solution that uses num_used items with "vj"
                        if sum_to_max_size[s - vj * num_used] + num_used > temp_sum_to_max_size[s]:
                            current_arr[s] = num_used
                            temp_sum_to_max_size[s] = sum_to_max_size[s - vj * num_used] + num_used
            sum_to_max_size = temp_sum_to_max_size
            data.append(current_arr)
        return sum_to_max_size, data

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds=None) -> Dict[float, int]:
        items = sorted(df[agg_col].values)  # to ensure there are no duplicates
        # make a histogram, sorted by the value.
        self.hist = sorted(list(df[agg_col].value_counts().items()), key=lambda x: x[0])

        sum_to_max_size, data = self.calc_knapsack(items, self.hist)
        result = dict(enumerate(sum_to_max_size))
        self.df = df
        self.agg_col = agg_col
        self.data = data
        return result

    def get_subset_histogram_for_sum(self, target_sum):
        """returns a list of tuples of (value, count) representing a histogram of the solution subset."""
        index = len(self.hist) - 1  # index of unique value
        while index >= 0:
            if self.data[index][target_sum] > 0:
                value = self.hist[index][0]
                count = self.data[index][target_sum]
                yield value, count
                target_sum -= value*count
            index -= 1

    def get_subset_for_value(self, required_value: float):
        if self.data is None:
            raise Exception("self.data empty when get_subset_for_value was called")
        solution_histogram = self.get_subset_histogram_for_sum(required_value)
        # build subset from solution histogram
        grouped_indices = {k: list(v) for k, v in self.df.groupby(self.agg_col).groups.items()}
        solution_indices = []
        for value, required_count in solution_histogram:
            solution_indices.extend(grouped_indices[value][:required_count])
        return solution_indices

        # ids_to_keep = []
        # needed_items = SortedDict()
        # for sum, key in H[H.keys()[-1]][1]:
        #     items_in_key = list(get_subset_with_sum(vals[key], data[key], sum))
        #     for item in items_in_key:
        #         print((key[0], item[0]), item[1])
        #         needed_items[(key[0], item[0])] = item[1]


class MedianAggregation(AggregationMem):

    def __init__(self):
        pass

    def _get_median_subset_odd(self, df: pd.DataFrame, agg_col: str, median: float) -> set[int]:
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

    def _get_median_subset_even(self, tuples: list[tuple], low_index: float, high_index: float) -> set[int]:
        median = (tuples[low_index][1] + tuples[high_index][1]) / 2
        N = len(tuples)

        if low_index < N - high_index - 1:
            subset = tuples[:low_index + 1] + tuples[high_index: high_index + low_index + 1]
        elif low_index > N - high_index - 1:
            subset = tuples[low_index + high_index - N + 1: low_index + 1] + tuples[high_index:]
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

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds=None) -> Dict[float, int]:
        median_subsets = {}
        df = df.sort_values(by=agg_col)
        unique_values = df[agg_col].unique()

        for value in tqdm(unique_values):
            median_subsets[value] = self._get_median_subset_odd(df, agg_col, value)
        tuples = list(df[agg_col].to_dict().items())  # tuples of index and agg_col value
        for low_index in tqdm(range(len(tuples))):
            # TODO: can prune more - if the low_index is too far to the left or high_index too far to the right
            if max_removed is not None:
                if low_index*2 < len(tuples) - max_removed: # TODO check with even/odd n and m
                    continue
                max_distance = min(len(tuples) - low_index, max_removed + 1)
            else:
                max_distance = len(tuples) - low_index
            for high_index in range(low_index + 1,
                                    low_index + max_distance):  # here we prune: don't consider indices which are too far apart
                # print(high_index, max_removed)
                if max_removed is not None and high_index*2 > len(tuples) + max_removed:
                    continue
                median = (tuples[low_index][1] + tuples[high_index][1]) / 2
                subset = self._get_median_subset_even(tuples, low_index, high_index)
                if median not in median_subsets or len(median_subsets[median]) < len(subset):
                    median_subsets[median] = subset
        self.median_subsets = median_subsets
        val_to_max_size = {v: len(subset) for v, subset in self.median_subsets.items()}
        return val_to_max_size

    def get_subset_for_value(self, required_value: float):
        return self.median_subsets[required_value]




class MedianAggregationOpt(AggregationMem):
    def __init__(self, parallelize=False):
        pass

    def get_medians(self, items, hist, max_removed: int = None):
        output = defaultdict(lambda: -1)  # median to num of remaining items
        data = defaultdict(lambda: ([], 0))  # median to (pivot list, num items on each side)
        # when keeping all items
        n = len(items)
        # single pivot
        start, end = 0, n
        if max_removed is not None:
            start = max(0, n//2 - max_removed//2 - 1)
            end = min(n, n//2 + max_removed//2 + 1)
        for index in tqdm(range(start, end)):
            remaining_on_each_side = min(index, n - index - 1)
            remaining_items = 2 * remaining_on_each_side + 1
            if output[items[index]] < remaining_items:
                output[items[index]] = remaining_items
                data[items[index]] = ([items[index]], remaining_on_each_side)  # pivots, how many each side
        # double adjacent pivot
        for index in tqdm(range(start, min(end, n - 1))):
            remaining_on_each_side = min(index, n - index - 2)
            remaining_items = 2 * remaining_on_each_side + 2
            median = (items[index] + items[index + 1]) / 2
            if output[median] < remaining_items:
                output[median] = remaining_items
                data[median] = ([items[index], items[index + 1]], remaining_on_each_side)  # pivots, how many each side
        # double nonadjacent pivot. For each possible median, we want the two closest pivots.
        remaining_on_the_left = 0
        for i in tqdm(range(len(hist))):
            remaining_on_the_left += hist[i][1]
            remaining_on_the_right = n - remaining_on_the_left
            if max_removed is not None and remaining_on_the_left * 2 <= n - max_removed:
                # i is too far to the left
                continue
            for j in range(i + 1, len(hist)):
                if max_removed is not None and remaining_on_the_right * 2 <= n - max_removed:
                    # j is too far to the right
                    continue
                median = (hist[i][0] + hist[j][0]) / 2
                if output[median] < 2 * min(remaining_on_the_left, remaining_on_the_right):
                    output[median] = 2 * min(remaining_on_the_left, remaining_on_the_right)
                    data[median] = ([hist[i][0], hist[j][0]], min(remaining_on_the_left,
                                                                  remaining_on_the_right) - 1)  # pivots, how many each side
                remaining_on_the_right -= hist[j][1]
        return output, data

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds=None) -> Dict[float, int]:
        items = sorted(df[agg_col].values)  # to ensure there are no duplicates
        # make a histogram, sorted by the value.
        self.hist = sorted(list(df[agg_col].value_counts().items()), key=lambda x: x[0])

        median_to_max_size, data = self.get_medians(items, self.hist, max_removed)
        self.df = df
        self.agg_col = agg_col
        self.data = data
        return median_to_max_size

    def get_subset_histogram_for_median(self, target):
        # TODO compare the expected number of removal to the actual set of tuples
        pivots, remaining_on_each_side = self.data[target]
        values_needed = []
        amount_needed = []
        remaining_on_the_left = remaining_on_each_side
        index = 0
        while remaining_on_the_left > 0:
            values_needed.append(self.hist[index][0])
            amount_needed.append(min(self.hist[index][1], remaining_on_the_left))
            remaining_on_the_left -= self.hist[index][1]
            index += 1
        needed_end = remaining_on_each_side
        index = len(self.hist) - 1
        while needed_end > 0:
            if self.hist[index][0] not in values_needed:
                values_needed.append(self.hist[index][0])
                amount_needed.append(0)
            amount_needed[values_needed.index(self.hist[index][0])] += min(self.hist[index][1], needed_end)
            needed_end -= self.hist[index][1]
            index -= 1
        # pivots
        for pivot in pivots:
            if pivot not in values_needed:
                values_needed.append(pivot)
                amount_needed.append(0)
            amount_needed[values_needed.index(pivot)] += 1
        for index in range(len(values_needed)):
            yield values_needed[index], amount_needed[index]

    def get_subset_for_value(self, required_value: float):
        if self.data is None:
            raise Exception("self.data empty when get_subset_for_value was called")
        solution_histogram = self.get_subset_histogram_for_median(required_value)
        # build subset from solution histogram
        grouped_indices = {k: list(v) for k, v in self.df.groupby(self.agg_col).groups.items()}
        solution_indices = []
        for value, required_count in solution_histogram:
            solution_indices.extend(grouped_indices[value][:required_count])
        return solution_indices


class AvgAggregation(AggregationMem):
    def __init__(self, parallelize=False):
        super().__init__()
        self.tuples = None
        self.subset_sizes = None

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds=None) -> Dict[float, int]:
        inf = len(df)+1
        self.tuples = list(df[agg_col].to_dict().items())  # tuples of index and agg_col value

        first_value = self.tuples[0][1]
        # subset_sizes is a dict of the form {j: {s: {k: subset size}}}, 
        # where j - the index of the tuple in an ordered list, s - the sum, and k - the size. subset_size is either k or -inf if no such subset exists.
        subset_sizes = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: -inf)))
        subset_sizes[0][0][0] = 0  # initialize with an empty size (size 0) having sum 0
        subset_sizes[0][first_value][1] = 1  # Initialize with sum first_value having the first tuple (subset size 1)

        for j in range(1, len(self.tuples)):
            value = self.tuples[j][1]  # value of the current tuple
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
        avg_subsets: Dict[float, int] = {}  # avg value to (size, sum)
        for current_sum in subset_sizes[len(self.tuples)-1]:
            for size in subset_sizes[len(self.tuples)-1][current_sum]:
                avg = 0 if size == 0 else current_sum / size
                if avg not in avg_subsets or avg_subsets[avg] < size:
                    avg_subsets[avg] = size
        return avg_subsets

    def get_subset_for_value(self, required_value: float):
        if self.subset_sizes is None:
            raise Exception("Subset sizes empty when get_subset_for_value was called")
        j = len(self.tuples) - 1
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
        c = best_size  # this is exactly how many tuples we will keep
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
            print(len(subset))
            print(required_size)
            raise Exception("returned subset size does not match DP table")
        if len(subset) != len(set(subset)):
            raise Exception("return subset has duplicate indices")
        actual_avg = np.mean([t[1] for t in self.tuples if t in subset])
        if np.abs(actual_avg - required_value) > ERROR_EPSILON:
            print(f"Mismatch in avg values: actual: {actual_avg} required: {required_value}")
        return subset



# Global for read-only access in subprocesses
shared_previous_layer = None


def init_worker(subset_sizes_j_minus_1):
    global shared_previous_layer
    shared_previous_layer = subset_sizes_j_minus_1


def process_current_sum(args):
    current_sum, value, max_removed = args
    global shared_previous_layer

    #current_result = defaultdict(lambda: -float('inf'))
    #current_result = defaultdict(lambda: defaultdict(lambda: -float('inf')))
    current_result = {}

    for current_size in shared_previous_layer[current_sum]:
        if max_removed is not None and current_size > max_removed:
            continue
        # Without removing the current tuple
        if current_sum not in current_result:
            current_result[current_sum] = {}
        current_result[current_sum][current_size] = current_size
        # With removing the current tuple
        if current_sum - value not in current_result:
            current_result[current_sum - value] = {}
        current_result[current_sum - value][current_size + 1] = current_size + 1
    return current_result


class AvgAggregationPruning(AggregationMem):
    def __init__(self, parallelize=False):
        super().__init__()
        self.tuples = None
        self.subset_sizes = None
        self.parallelize = parallelize

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds=None) -> Dict[float, int]:
        inf = len(df) + 1
        self.tuples = list(df[agg_col].to_dict().items())  # tuples of index and agg_col value

        s = df[agg_col].sum()
        first_value = self.tuples[0][1]
        # subset_sizes is a dict of the form {j: {s: {k: subset size}}},
        # where j - the index of the tuple in an ordered list, s - the sum, and k - the size (of the removed subset).
        # subset_size is either k or -inf if no such subset exists.
        # TODO: make this into a list of subset sizes. Meaning: {j: {s: {k1, k2,...}}}
        subset_sizes = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: -inf)))
        subset_sizes[0][s][0] = 0  # initialize with an empty size (size 0) having sum 0
        subset_sizes[0][s - first_value][1] = 1  # Initialize with sum-first_value - when removing the first tuple (subset size 1)

        #max_removed = len(df) - min_subset_size if min_subset_size is not None else None
        iterations_over_time_limit = 0
        with tqdm(range(1, len(self.tuples))) as t:
            for j in t:
                d = t.format_dict
                if d['rate'] is not None:
                    remaining_time_estimate = (d['total'] - d['n']) / d['rate']
                    if time_cutoff_seconds is not None and remaining_time_estimate > time_cutoff_seconds:
                        iterations_over_time_limit += 1
                    if iterations_over_time_limit > 10:
                        print(d)
                        print(f"estimated time left is too high: {remaining_time_estimate / 60} minutes, exiting")
                        sys.exit()
                value = self.tuples[j][1]  # value of the current tuple
                current_subset_sizes = defaultdict(
                    lambda: defaultdict(lambda: -inf))  # Avoid modifying dict while iterating

                if self.parallelize:
                    previous_layer = subset_sizes[j - 1]

                    # Prepare args for each current_sum
                    pool_args = [(current_sum, value, max_removed) for current_sum in previous_layer.keys()]
                    num_workers = min(cpu_count(), NUM_PROCESSES)

                    with Pool(processes=num_workers, initializer=init_worker, initargs=(previous_layer,)) as pool:
                        results = list(pool.map(process_current_sum, pool_args))

                    # Merge results into current_subset_sizes
                    for res in results:
                        for sum_key, sizes_dict in res.items():
                            for size in sizes_dict.keys():
                                current_subset_sizes[sum_key][size] = size
                else:
                    for current_sum in subset_sizes[j - 1].keys():
                        for current_size in subset_sizes[j - 1][current_sum]:
                            if max_removed is not None and current_size > max_removed:
                                continue
                            if current_sum not in current_subset_sizes or current_size not in current_subset_sizes[current_sum]:
                                # without removing the current tuple
                                current_subset_sizes[current_sum][current_size] = current_size

                            if current_sum - value not in current_subset_sizes or (current_size + 1) not in \
                                    current_subset_sizes[current_sum - value]:
                                # with removing the current tuple
                                current_subset_sizes[current_sum - value][current_size + 1] = current_size + 1
                subset_sizes[j] = current_subset_sizes
        self.subset_sizes = subset_sizes
        avg_subsets: Dict[float, int] = {}  # avg value to #tuples to keep
        for current_sum in subset_sizes[len(self.tuples) - 1]:
            for size_removed in subset_sizes[len(self.tuples) - 1][current_sum]:
                size = len(df) - size_removed
                avg = 0 if size == 0 else current_sum / size
                if avg not in avg_subsets or avg_subsets[avg] < size:
                    avg_subsets[avg] = size
        return avg_subsets

    def get_subset_for_value(self, required_value: float):
        if self.subset_sizes is None:
            raise Exception("Subset sizes empty when get_subset_for_value was called")

        # get sum and size for the required_value
        best_sum, best_size = 0, 0
        # subset_sizes[n] is a dict of the form {s: {k: subset size}},
        # s - the sum, and k - the size (of the removed subset).
        # subset_size is either k or -inf if no such subset exists.
        for current_sum in self.subset_sizes[len(self.tuples) - 1]:
            for removed_size in self.subset_sizes[len(self.tuples) - 1][current_sum]:
                size = len(self.tuples) - removed_size
                avg = 0 if size == 0 else current_sum / size
                if np.abs(avg - required_value) <= ERROR_EPSILON:
                    if size > best_size:
                        best_sum, best_size = current_sum, size
        best_removed_size = len(self.tuples) - best_size

        j = len(self.tuples) - 1
        # for sanity check - the expected size
        required_size = self.subset_sizes[j][best_sum][best_removed_size]
        if required_size != best_removed_size:
            print("avg: mismatch in subset sizes")
        s = best_sum
        c = best_removed_size  # this is exactly how many tuples we will remove
        orig_sum = sum([t[1] for t in self.tuples])

        subset = []
        for j in range(len(self.tuples) - 1, -1, -1):  # j=n,...,0
            if c <= 0:
                break
            tuple_j_value = self.tuples[j][1]
            if j == 0:
                if s + tuple_j_value == orig_sum:
                    subset.append(self.tuples[j][0])
                    break
                elif s == orig_sum:
                    break
            else:
                not_remove_j = self.subset_sizes[j - 1][s][c]
                remove_j = self.subset_sizes[j - 1][s + tuple_j_value][c - 1] + 1
                if remove_j >= not_remove_j:
                    subset.append(self.tuples[j][0])
                    s += tuple_j_value
                    c -= 1
        keep = [t[0] for t in self.tuples if t[0] not in subset]
        #print(f"removed values: {[t[1] for t in self.tuples if t[0] in subset]}")
        actual_avg = np.mean([t[1] for t in self.tuples if t[0] in keep])
        if len(subset) != required_size:
            print(f"removed: {len(subset)}")
            print(f"expected removed size from dp table: {required_size}")
            raise Exception("Avg prune: returned subset size does not match DP table")
        if len(subset) != len(set(subset)):
            raise Exception("return subset has duplicate indices")
        if np.abs(actual_avg - required_value) > ERROR_EPSILON:
            print(f"Mismatch in avg values: actual: {actual_avg} required: {required_value}")
        return keep


class AvgAggregationPruningHistogram(AggregationMem):
    def __init__(self):
        self.dp = None
        self.total_count = 0
        self.total_sum = 0
        self.hist = None
        self.came_from = None
        self.df = None
        self.agg_col = None

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, max_removed: int = None,
                                 time_cutoff_seconds: int = None) -> Dict[float, int]:
        self.df = df
        self.agg_col = agg_col
        self.hist = sorted(list(df[agg_col].value_counts().items()), key=lambda x: x[0])
        self.total_count = len(df)
        self.total_sum = df[agg_col].sum()
        # self.histogram = Counter(df[agg_col])
        # self.total_count = sum(self.histogram.values())
        # self.total_sum = sum(v * c for v, c in self.hist.items())

        dp = defaultdict(lambda: defaultdict(lambda: False))  # dp[sum][removed_count] = True/False
        came_from = defaultdict(dict)  # came_from[sum][removed_count] = (prev_sum, prev_removed, value, times_used)
        dp[self.total_sum][0] = True

        for value, count in self.hist:
            new_dp = defaultdict(lambda: defaultdict(lambda: False))
            new_came_from = came_from.copy()

            for curr_sum in dp:
                for curr_removed in dp[curr_sum]:
                    for k in range(0, count + 1):
                        new_sum = curr_sum - k * value
                        new_removed = curr_removed + k
                        if max_removed is not None and new_removed > max_removed:
                            break
                        new_dp[new_sum][new_removed] = True
                        if k > 0:
                            if new_sum not in new_came_from or new_removed not in new_came_from[new_sum]:
                                # if we reach the same sum and count again, keep the previous method of getting there.
                                new_came_from[new_sum][new_removed] = (curr_sum, curr_removed, value, k)
            dp = new_dp
            for s in new_came_from:
                for r in new_came_from[s]:
                    came_from[s][r] = new_came_from[s][r]

        self.dp = dp
        self.came_from = came_from

        avg_subsets = {}
        for s in dp:
            for removed in dp[s]:
                kept = self.total_count - removed
                avg = 0 if kept == 0 else s / kept
                if avg not in avg_subsets or avg_subsets[avg] < kept:
                    avg_subsets[avg] = kept
        return avg_subsets

    def get_subset_for_value(self, required_value: float, epsilon: float = 1e-6) -> list:
        if self.dp is None or self.came_from is None:
            raise ValueError("Must run compute_max_subset_sizes first")

        best_sum, best_removed = None, None
        best_kept = -1
        for s in self.dp:
            for removed in self.dp[s]:
                kept = self.total_count - removed
                avg = 0 if kept == 0 else s / kept
                if abs(avg - required_value) <= epsilon and kept > best_kept:
                    best_sum, best_removed = s, removed
                    best_kept = kept
        print(f"get_subset_for_value: {best_sum}, {best_removed}")

        if best_sum is None:
            raise ValueError("No matching subset found for requested average")
        if best_removed == 0:
            return self.df.index

        # Backtrack using came_from
        values_to_remove = defaultdict(int)
        s, r = best_sum, best_removed
        while s in self.came_from and r in self.came_from[s]:
            #print(f"in the loop: {s}, {r}, {self.came_from[s][r]}")
            prev_s, prev_r, val, count = self.came_from[s][r]
            values_to_remove[val] += count
            s, r = prev_s, prev_r
        #print(values_to_remove)
        #tot = sum([v*c for v,c in values_to_remove.items()])
        #print(f'sum of values to remove: {tot}')

        # Map values to original indices
        grouped_indices = {k: list(v) for k, v in self.df.groupby(self.agg_col).groups.items()}
        removal_indices = []
        for value, required_count in values_to_remove.items():
            available = [x for x in self.hist if x[0] == value][0][1]
            if required_count > available:
                raise ValueError(f"Solution used too many instances of value: {value}, used: {required_count} out of {available}")
            print(f"value:{value}, required_count: {required_count}, available: {[x for x in self.hist if x[0] == value]}")
            removal_indices.extend(grouped_indices[value][:required_count])
        indices_to_keep = list(set(self.df.index).difference(removal_indices))
        #print(f"to keep: {len(indices_to_keep)}, to remove: {len(removal_indices)}")
        print(removal_indices)
        #print(f"sum of removed indexes: {self.df.loc[removal_indices][self.agg_col].sum()}")
        actual_avg = self.df.loc[indices_to_keep, self.agg_col].mean()
        if abs(actual_avg - required_value) > epsilon:
            raise ValueError(f"Warning: mismatch in reconstructed avg: expected {required_value}, got {actual_avg}")
        return indices_to_keep
