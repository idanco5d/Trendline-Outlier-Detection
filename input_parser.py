import argparse
from dataclasses import dataclass
from typing import List, Union

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from aggregations import get_avg_subsets, get_count_subsets, get_count_distinct_subsets, get_max_subsets, \
    get_min_subsets, get_sum_subsets, get_median_subsets, AggregationFunction
from aggregations_pruning import get_sum_subsets_pruning, get_avg_subsets_pruning, get_median_subsets_pruning, AggregationPruningFunction

AGGREGATIONS = {
    'AVG': get_avg_subsets,
    'COUNT': get_count_subsets,
    'COUNT_DISTINCT': get_count_distinct_subsets,
    'MAX': get_max_subsets,
    'MIN': get_min_subsets,
    'SUM': get_sum_subsets,
    'MEDIAN': get_median_subsets,
}
PRUNING_AGGREGATIONS = {
    'SUM': get_sum_subsets_pruning,
    'AVG': get_avg_subsets_pruning,
    'MEDIAN': get_median_subsets_pruning
}


@dataclass
class Input:
    df: pd.DataFrame
    group_cols: List[str]
    agg_col: str
    aggregation: Union[AggregationFunction, AggregationPruningFunction]
    prune: int = None


def parse_input() -> Input:
    args = get_input_arguments()
    df = pd.read_csv(args.dataset_file_name)
    agg_col = args.aggregation_column
    check_agg_col(df, agg_col)
    group_cols = args.grouping_columns
    check_group_cols(df, group_cols)
    prune = args.prune
    aggregation = get_aggregation_function(args.aggregation_function, is_pruning=prune is not None)

    return Input(df=df, group_cols=group_cols, agg_col=agg_col, aggregation=aggregation, prune=prune)


def get_input_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'aggregation_function',
        type=str,
        help=f'Chosen aggregation function: {", ".join(AGGREGATIONS.keys())}'
    )
    parser.add_argument('dataset_file_name', type=str, help='Your dataset csv file')
    parser.add_argument('aggregation_column', type=str, help='Name of the aggregated column')
    parser.add_argument('grouping_columns', nargs='+', type=str, help='Names of the grouping attributes')
    parser.add_argument('--prune', type=int, metavar='N', help='Prune with an integer parameter N')

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


def get_aggregation_function(
        function_name: str, is_pruning: bool = False
) -> Union[AggregationFunction, AggregationPruningFunction]:
    if is_pruning:
        if function_name not in PRUNING_AGGREGATIONS.keys():
            raise ValueError(f'Unrecognized aggregation function for pruning: {function_name}')
        return PRUNING_AGGREGATIONS[function_name]

    if function_name not in AGGREGATIONS.keys():
        raise ValueError(f'Unrecognized aggregation function: {function_name}')
    return AGGREGATIONS[function_name]
