import argparse
from typing import List

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from aggregations.aggregation import Aggregation
from aggregations.average import Average
from aggregations.count_distinct import CountDistinct
from aggregations.count import Count
from aggregations.max import Max
from aggregations.min import Min
from aggregations.sum import Sum


def parse_input() -> (Aggregation, pd.DataFrame, int, DataFrameGroupBy):
    args = get_input_arguments()
    df = pd.read_csv(args.datasetFileName)
    check_agg_col(df, args.aggregationColumn)

    return (
        get_aggregation_function_from_input(args.aggregationFunction),
        df,
        args.aggregationColumn,
        group_frame_by_attributes(df, args.groupingColumns, args.aggregationColumn)
    )


def get_input_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('aggregationFunction', type=str, help='Chosen aggregation function')
    parser.add_argument('datasetFileName', type=str, help='Your dataset csv file')
    parser.add_argument('aggregationColumn', type=str, help='Name of the aggregated column')
    parser.add_argument('groupingColumns', nargs='+', type=str, help='Names of the grouping attributes')

    return parser.parse_args()


def get_aggregation_function_from_input(function_name: str) -> Aggregation:
    match function_name:
        case "MAX":
            return Max()
        case "MIN":
            return Min()
        case "COUNT":
            return Count()
        case "COUNT_DISTINCT":
            return CountDistinct()
        case "SUM":
            return Sum()
        case "AVG":
            return Average()

    raise ValueError(f"Unrecognized aggregation function: {function_name}")


def check_agg_col(data_df: pd.DataFrame, agg_col: str):
    if agg_col not in data_df.columns:
        raise ValueError('Invalid aggregation attribute name')


def group_frame_by_attributes(df: pd.DataFrame, grouping_cols: List[str], agg_col: str) -> DataFrameGroupBy:
    try:
        df = df.sort_values(by=grouping_cols + [agg_col])
        df_grouped = df.groupby(grouping_cols)
    except KeyError:
        raise ValueError('Invalid grouping attribute name')

    return df_grouped
