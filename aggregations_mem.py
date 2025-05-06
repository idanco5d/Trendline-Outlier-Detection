from itertools import combinations
from typing import Dict, Protocol
from collections import defaultdict
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
from tqdm import tqdm

from constants import *
# ERROR_EPSILON = 0.00001
# NUM_PROCESSES = 20


class AggregationMem(object):
    def __init__(self, parallelize=False):
        pass
        
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str) -> Dict[float, int]:
        pass
    
    def get_subset_for_value(self, required_value: float):
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
    def __init__(self, parallelize=False):
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
                    # TODO understand this part
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

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, min_subset_size: int = None) -> Dict[float, int]:
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


class MedianAggregationOpt(AggregationMem):
    def __init__(self, parallelize=False):
        pass

    def get_medians(self, items, hist):
        output = defaultdict(lambda: -1)  # median to num of remaining items
        data = defaultdict(lambda: ([], 0))  # median to (pivot list, num items on each side)
        # when keeping all items
        n = len(items)
        # single pivot
        for index in range(n):
            remaining_on_each_side = min(index, n - index - 1)
            remaining_items = 2 * remaining_on_each_side + 1
            if output[items[index]] < remaining_items:
                output[items[index]] = remaining_items
                data[items[index]] = ([items[index]], remaining_on_each_side)  # pivots, how many each side
        # double adjacent pivot
        for index in range(n - 1):
            remaining_on_each_side = min(index, n - index - 2)
            remaining_items = 2 * remaining_on_each_side + 2
            median = (items[index] + items[index + 1]) / 2
            if output[median] < remaining_items:
                output[median] = remaining_items
                data[median] = ([items[index], items[index + 1]], remaining_on_each_side)  # pivots, how many each side
        # double nonadjacent pivot. For each possible median, we want the two closest pivots.
        remaining_on_the_left = 0
        for i in range(len(hist)):
            remaining_on_the_left += hist[i][1]
            remaining_on_the_right = n - remaining_on_the_left
            for j in range(i + 1, len(hist)):
                median = (hist[i][0] + hist[j][0]) / 2
                if output[median] < 2 * min(remaining_on_the_left, remaining_on_the_right):
                    output[median] = 2 * min(remaining_on_the_left, remaining_on_the_right)
                    data[median] = ([hist[i][0], hist[j][0]], min(remaining_on_the_left,
                                                                  remaining_on_the_right) - 1)  # pivots, how many each side
                remaining_on_the_right -= hist[j][1]
        return output, data

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str) -> Dict[float, int]:
        items = sorted(df[agg_col].values)  # to ensure there are no duplicates
        # make a histogram, sorted by the value.
        self.hist = sorted(list(df[agg_col].value_counts().items()), key=lambda x: x[0])

        median_to_max_size, data = self.get_medians(items, self.hist)
        self.df = df
        self.agg_col = agg_col
        self.data = data
        return median_to_max_size

    def get_subset_histogram_for_median(self, target):
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

    
    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str) -> Dict[float, int]:
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

    def compute_max_subset_sizes(self, df: pd.DataFrame, agg_col: str, min_subset_size: int = None) -> Dict[float, int]:
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

        max_removed = len(df) - min_subset_size if min_subset_size is not None else None

        for j in tqdm(range(1, len(self.tuples))):
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
