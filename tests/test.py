import unittest
from typing import Callable, Dict

import pandas as pd
from pandas._testing import assert_frame_equal

from aggregations import get_avg_subsets, get_count_subsets, get_count_distinct_subsets, get_max_subsets, \
    get_min_subsets, get_sum_subsets
from optimal_subset_with_constraint import get_optimal_subset


class TestOptimalSolution(unittest.TestCase):
    def test_max(self):
        actual_solution = get_optimal_solution("max/max_initial_file.csv", get_max_subsets)
        expected_solution = pd.read_csv("max/max_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_min(self):
        actual_solution = get_optimal_solution("min/min_initial_file.csv", get_min_subsets)
        expected_solution = pd.read_csv("min/min_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_count(self):
        actual_solution = get_optimal_solution("count/count_initial_file.csv", get_count_subsets)
        expected_solution = pd.read_csv("count/count_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_count_distinct(self):
        actual_solution = get_optimal_solution("count_distinct/count_distinct_initial_file.csv",
                                               get_count_distinct_subsets)
        expected_solution = pd.read_csv("count_distinct/count_distinct_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_sum(self):
        actual_solution = get_optimal_solution("sum/sum_initial_file.csv", get_sum_subsets)
        expected_solution = pd.read_csv("sum/sum_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_average(self):
        actual_solution = get_optimal_solution("average/average_initial_file.csv", get_avg_subsets)
        expected_solution = pd.read_csv("average/average_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)


def get_optimal_solution(input_file_name: str, agg: Callable[[pd.DataFrame, str], Dict[float, set]]) -> pd.DataFrame:
    df = pd.read_csv(input_file_name)
    result_df, removed_df = get_optimal_subset(df, ['grouping_1', 'grouping_2'], 'aggregator', agg)
    return result_df


def assert_data_frames_equal(df1: pd.DataFrame, df2: pd.DataFrame):
    assert_frame_equal(df1.reset_index(drop=True), df2.reset_index(drop=True), False)


if __name__ == '__main__':
    unittest.main()
