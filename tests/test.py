import unittest

import pandas as pd
from pandas._testing import assert_frame_equal

from aggregations.aggregation import Aggregation
from aggregations.average import Average
from aggregations.count_distinct import CountDistinct
from aggregations.count import Count
from aggregations.max import Max
from aggregations.min import Min
from aggregations.sum import Sum
from input_parser import group_frame_by_attributes
from optimal_subset_with_constraint import calculate_optimal_subset_with_constraint


class TestOptimalSolution(unittest.TestCase):
    def test_max(self):
        actual_solution = get_optimal_solution("max/max_initial_file.csv", Max())
        expected_solution = pd.read_csv("max/max_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_min(self):
        actual_solution = get_optimal_solution("min/min_initial_file.csv", Min())
        expected_solution = pd.read_csv("min/min_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_count(self):
        actual_solution = get_optimal_solution("count/count_initial_file.csv", Count())
        expected_solution = pd.read_csv("count/count_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_count_distinct(self):
        actual_solution = get_optimal_solution("count_distinct/count_distinct_initial_file.csv", CountDistinct())
        expected_solution = pd.read_csv("count_distinct/count_distinct_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_sum(self):
        actual_solution = get_optimal_solution("sum/sum_initial_file.csv", Sum())
        expected_solution = pd.read_csv("sum/sum_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_average(self):
        actual_solution = get_optimal_solution("average/average_initial_file.csv", Average())
        expected_solution = pd.read_csv("average/average_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)


def get_optimal_solution(input_file_name: str, function: Aggregation) -> pd.DataFrame:
    data = pd.read_csv(input_file_name)
    grouped_rows_by_value = group_frame_by_attributes(data, ['grouping_1', 'grouping_2'], 'aggregator')
    return calculate_optimal_subset_with_constraint(
        grouped_rows_by_value,
        function,
        'aggregator'
    )


def assert_data_frames_equal(df1: pd.DataFrame, df2: pd.DataFrame):
    assert_frame_equal(reset_df_index(df1), reset_df_index(df2), False)


def reset_df_index(df: pd.DataFrame) -> pd.DataFrame:
    return df.reset_index(drop=True)


if __name__ == '__main__':
    unittest.main()
