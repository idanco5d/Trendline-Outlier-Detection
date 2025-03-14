import unittest
from typing import Callable, List

import pandas as pd

from aggregations import get_avg_subsets, get_count_distinct_subsets, get_max_subsets, \
    get_min_subsets, get_sum_subsets, get_count_subsets, get_median_subsets
from aggregations_pruning import get_sum_subsets_pruning, get_avg_subsets_pruning
from optimal_subset_with_constraint import get_optimal_subset
from optimal_subset_with_constraints_pruning import get_optimal_subset_pruning

GROUP_COLS = ['grouping_1', 'grouping_2']
AGG_COL = 'aggregator'


def get_min(df: pd.DataFrame) -> pd.Series:
    return df.min()


def get_max(df: pd.DataFrame) -> pd.Series:
    return df.max()


def get_count(df: pd.DataFrame) -> pd.Series:
    return df.count()


def get_count_distinct(df: pd.DataFrame) -> pd.Series:
    return df.nunique()


def get_sum(df: pd.DataFrame) -> pd.Series:
    return df.sum()


def get_avg(df: pd.DataFrame) -> pd.Series:
    return df.mean()


def get_median(df: pd.DataFrame) -> pd.Series:
    return df.median()


TEST_PARAMS = {
    'max': [get_max_subsets, get_max],
    'min': [get_min_subsets, get_min],
    'count': [get_count_subsets, get_count],
    'count_distinct': [get_count_distinct_subsets, get_count_distinct],
    'sum': [get_sum_subsets, get_sum],
    'avg': [get_avg_subsets, get_avg],
    'median': [get_median_subsets, get_median]
}

TEST_PRUNING_PARAMS = {
    'sum': [get_sum_subsets_pruning, get_sum],
    'avg': [get_avg_subsets_pruning, get_avg]
}


def check_trend(
        df: pd.DataFrame, group_cols: List[str], agg_col: str, agg: Callable[[pd.DataFrame], pd.Series]
) -> bool:
    agg_df = agg(
        df[group_cols + [agg_col]]
        .groupby(group_cols)
    )[agg_col].to_frame()
    sorted_df = agg_df.sort_values(by=agg_col)
    return agg_df.equals(sorted_df)


def check_solution(
        expected_df: pd.DataFrame, result_df: pd.DataFrame, agg: Callable[[pd.DataFrame], pd.Series]
) -> None:
    if not check_trend(expected_df, GROUP_COLS, AGG_COL, agg):
        raise Exception('Expected does not satisfy trend')
    if not check_trend(result_df, GROUP_COLS, AGG_COL, agg):
        raise Exception('Solution does not satisfy trend')

    if len(result_df) > len(expected_df):
        raise Exception('Solution bigger than expected')
    if len(result_df) < len(expected_df):
        raise Exception('Solution smaller than expected')


def test_agg(agg_name: str):
    subset_agg_func, agg_func = TEST_PARAMS[agg_name]

    input_file_name = f'{agg_name}/input.csv'
    input_df = pd.read_csv(input_file_name)
    expected_file_name = f'{agg_name}/expected.csv'
    expected_df = pd.read_csv(expected_file_name)

    result_df, removed_df = get_optimal_subset(input_df, GROUP_COLS, AGG_COL, subset_agg_func)
    check_solution(expected_df, result_df, agg_func)


def test_agg_pruning(agg_name: str, max_removed: int):
    subset_agg_func, agg_func = TEST_PRUNING_PARAMS[agg_name]

    input_file_name = f'{agg_name}/input.csv'
    input_df = pd.read_csv(input_file_name)
    expected_file_name = f'{agg_name}/expected.csv'
    expected_df = pd.read_csv(expected_file_name)

    result_df, removed_df = get_optimal_subset_pruning(
        input_df, GROUP_COLS, AGG_COL, subset_agg_func, max_removed
    )
    check_solution(expected_df, result_df, agg_func)


class TestOptimalSolution(unittest.TestCase):
    def test_max(self):
        test_agg('max')

    def test_min(self):
        test_agg('min')

    def test_count(self):
        test_agg('count')

    def test_count_distinct(self):
        test_agg('count_distinct')

    def test_sum(self):
        test_agg('sum')

    def test_sum_pruning(self):
        test_agg_pruning('sum', max_removed=2)

    def test_avg(self):
        test_agg('avg')

    def test_avg_pruning(self):
        test_agg_pruning('avg', max_removed=2)

    def test_median(self):
        test_agg('median')


if __name__ == '__main__':
    unittest.main()
