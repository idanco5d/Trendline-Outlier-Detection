import unittest

import pandas as pd
from pandas._testing import assert_frame_equal

from aggregation_functions.aggregation_function import AggregationFunction
from aggregation_functions.average_function import AverageFunction
from aggregation_functions.count_distinct_function import CountDistinctFunction
from aggregation_functions.count_function import CountFunction
from aggregation_functions.max_function import MaxFunction
from aggregation_functions.min_function import MinFunction
from aggregation_functions.sum_function import SumFunction
from input_parser import parse_csv_to_data_frame, group_frame_by_attributes
from optimal_subset_with_constraint import calculate_optimal_subset_with_constraint


class TestOptimalSolution(unittest.TestCase):
    def test_max(self):
        actual_solution = get_optimal_solution("max/max_initial_file.csv", MaxFunction())
        expected_solution = pd.read_csv("max/max_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_min(self):
        actual_solution = get_optimal_solution("min/min_initial_file.csv", MinFunction())
        expected_solution = pd.read_csv("min/min_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_count(self):
        actual_solution = get_optimal_solution("count/count_initial_file.csv", CountFunction())
        expected_solution = pd.read_csv("count/count_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_count_distinct(self):
        actual_solution = get_optimal_solution("count_distinct/count_distinct_initial_file.csv", CountDistinctFunction())
        expected_solution = pd.read_csv("count_distinct/count_distinct_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_sum(self):
        actual_solution = get_optimal_solution("sum/sum_initial_file.csv", SumFunction())
        expected_solution = pd.read_csv("sum/sum_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)

    def test_average(self):
        actual_solution = get_optimal_solution("average/average_initial_file.csv", AverageFunction())
        expected_solution = pd.read_csv("average/average_expected_result.csv")
        assert_data_frames_equal(actual_solution, expected_solution)


def get_optimal_solution(input_file_name: str, function: AggregationFunction) -> pd.DataFrame:
    data = parse_csv_to_data_frame(input_file_name)
    grouped_rows_by_value = group_frame_by_attributes(data, ['grouping_1', 'grouping_2'], "aggregator")
    return calculate_optimal_subset_with_constraint(
        grouped_rows_by_value,
        function,
        3
    )


def assert_data_frames_equal(df1: pd.DataFrame, df2: pd.DataFrame):
    assert_frame_equal(reset_df_index(df1), reset_df_index(df2), False)


def reset_df_index(df: pd.DataFrame) -> pd.DataFrame:
    return df.reset_index(drop=True)


if __name__ == '__main__':
    unittest.main()
