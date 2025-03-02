import argparse
from typing import List, Callable, Dict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from aggregations import get_avg_subsets, get_count_subsets, get_count_distinct_subsets, get_max_subsets, \
    get_min_subsets, get_sum_subsets, get_median_subsets

AGGREGATIONS = {
    'AVG': get_avg_subsets,
    'COUNT': get_count_subsets,
    'COUNT_DISTINCT': get_count_distinct_subsets,
    'MAX': get_max_subsets,
    'MIN': get_min_subsets,
    'SUM': get_sum_subsets,
    'MEDIAN': get_median_subsets,
}


def parse_input() -> (pd.DataFrame, List[str], str, Callable[[pd.DataFrame, str], Dict[float, set]]):
    args = get_input_arguments()
    df = pd.read_csv(args.dataset_file_name)
    agg_col = args.aggregation_column
    check_agg_col(df, agg_col)
    group_cols = args.grouping_columns
    check_group_cols(df, group_cols)
    aggregation = get_aggregation_function(args.aggregation_function)

    return df, group_cols, agg_col, aggregation


def get_input_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('aggregation_function', type=str, help='Chosen aggregation function')
    parser.add_argument('dataset_file_name', type=str, help='Your dataset csv file')
    parser.add_argument('aggregation_column', type=str, help='Name of the aggregated column')
    parser.add_argument('grouping_columns', nargs='+', type=str, help='Names of the grouping attributes')

    return parser.parse_args()


def check_agg_col(data_df: pd.DataFrame, agg_col: str):
    if agg_col not in data_df.columns:
        raise ValueError(f'Invalid aggregation column name: {agg_col}')


def check_group_cols(data_df: pd.DataFrame, group_cols: List[str]):
    for group_col in group_cols:
        if group_col not in data_df.columns:
            raise ValueError(f'Invalid group column name: {group_col}')


def group_frame_by_attributes(df: pd.DataFrame, grouping_cols: List[str], agg_col: str) -> DataFrameGroupBy:
    try:
        df = df.sort_values(by=grouping_cols + [agg_col])
        df_grouped = df.groupby(grouping_cols)
    except KeyError:
        raise ValueError('Invalid grouping attribute name')

    return df_grouped


def get_aggregation_function(function_name: str) -> Callable[[pd.DataFrame, str], Dict[float, set]]:
    if function_name not in AGGREGATIONS.keys():
        raise ValueError(f"Unrecognized aggregation function: {function_name}")
    return AGGREGATIONS[function_name]
