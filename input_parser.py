import argparse
import os
from dataclasses import dataclass
from typing import List, Union, Optional

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from aggregations import get_avg_subsets, get_count_subsets, get_count_distinct_subsets, get_max_subsets, \
    get_min_subsets, get_sum_subsets, get_median_subsets, AggregationFunction
from aggregations_pruning import get_sum_subsets_pruning, get_avg_subsets_pruning, get_median_subsets_pruning, AggregationPruningFunction

from aggregations_mem import AggregationMem, SumAggregation, SumAggregationOpt, AvgAggregation, AvgAggregationPruning, \
    MedianAggregationOpt, MedianAggregation, MaxAggregation, AvgAggregationPruningHistogram, CountAggregation, CountDistinctAggregation

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

MEM_AGGREGATIONS = {
    #'SUM': SumAggregation,
    'SUM': SumAggregationOpt,
    'AVG': AvgAggregation,
    'MEDIAN': MedianAggregationOpt,
}

MEM_AND_PRUNING_AGGREGATIONS = {
    'AVG': AvgAggregationPruning,
    'SUM': SumAggregation,
    'MEDIAN': MedianAggregation,
    'MAX': MaxAggregation,  # Agg pack pruning not available
    'COUNT': CountAggregation,
    'COUNT_DISTINCT': CountDistinctAggregation,
}

OPTIMIZED_AGGREGATIONS = {
    'SUM': SumAggregationOpt,  # agg pack pruning not available
    'MEDIAN': MedianAggregationOpt,
    'AVG': AvgAggregationPruningHistogram,
}

@dataclass
class RawArgs:
    aggregation_function: str
    dataset_file_name: str
    aggregation_column: str
    grouping_columns: list[str]
    output_folder: str
    prune_aggpack_by_greedy: Optional[int] = None
    prune_dp_by_greedy: Optional[int] = None
    prune_h: Optional[bool] = None
    mem_opt: Optional[bool] = None
    agg_pack_opt: Optional[bool] = None
    cutoff_seconds: Optional[int] = None


@dataclass
class Input:
    df: pd.DataFrame
    group_cols: List[str]
    agg_col: str
    aggregation: Union[AggregationFunction, AggregationPruningFunction, AggregationMem]
    output_folder: str
    orig_fname: str
    agg_name: str
    mem_opt: bool = False
    agg_pack_opt: bool = False
    prune_h: bool = None
    prune_aggpack_by_greedy: int = False
    prune_dp_by_greedy: int = False
    time_cutoff_seconds: int = None


def parse_input(initial_args: RawArgs | None = None) -> Input:
    args = get_input_arguments() if initial_args is None else initial_args
    df = pd.read_csv(args.dataset_file_name)
    orig_fname = os.path.basename(args.dataset_file_name)
    agg_col = args.aggregation_column
    check_agg_col(df, agg_col)
    group_cols = args.grouping_columns
    check_group_cols(df, group_cols)
    prune_dp_by_greedy = args.prune_dp_by_greedy
    prune_aggpack_by_greedy = args.prune_aggpack_by_greedy
    prune_h = args.prune_h
    mem_opt = args.mem_opt
    agg_name = args.aggregation_function
    aggregation = get_aggregation_function(args.aggregation_function, args.agg_pack_opt)
    if (args.aggregation_function == 'SUM' and args.agg_pack_opt and prune_aggpack_by_greedy):
        raise Exception("Unsupported combination: sum, agg_pack_opt, prune_aggpack_by_greedy")
    elif (args.aggregation_function == 'MAX' and (args.agg_pack_opt or prune_aggpack_by_greedy)):
        raise Exception(f"Unsupported combination: max has no aggpack optimizations. agg_pack_opt: {args.agg_pack_opt}, by greedy: {prune_aggpack_by_greedy}")
    time_cutoff_seconds = args.cutoff_seconds
    return Input(df=df, group_cols=group_cols, agg_col=agg_col, aggregation=aggregation,
                 mem_opt=mem_opt,
                 prune_h=prune_h, prune_dp_by_greedy=prune_dp_by_greedy, prune_aggpack_by_greedy=prune_aggpack_by_greedy,
                 time_cutoff_seconds=time_cutoff_seconds,
                 output_folder=args.output_folder, orig_fname=orig_fname, agg_name=agg_name)


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
    parser.add_argument('--prune_aggpack_by_greedy', type=int, metavar='N', help='Prune with an integer parameter N')
    parser.add_argument('--prune_dp_by_greedy', type=int, metavar='N', help='Prune with an integer parameter N')
    parser.add_argument('--prune_h', action=argparse.BooleanOptionalAction)
    parser.set_defaults(prune_h=False)
    parser.add_argument('--mem_opt', action=argparse.BooleanOptionalAction)
    parser.set_defaults(mem_opt=False)
    parser.add_argument('--agg_pack_opt', action=argparse.BooleanOptionalAction)
    parser.set_defaults(agg_pack_opt=False)
    parser.add_argument('--cutoff_seconds', type=int, metavar='N', help='time cutoff in seconds')
    parser.add_argument('--output_folder', type=str, help='path to save results', required=True)

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


def get_aggregation_function(function_name: str, agg_pack_opt: bool) -> AggregationMem:
    if agg_pack_opt:
        if function_name not in OPTIMIZED_AGGREGATIONS:
            raise ValueError(f'Unrecognized aggregation function for optimized aggregation: {function_name}')
        return OPTIMIZED_AGGREGATIONS[function_name]
    # Otherwise - naive agg packing
    if function_name not in MEM_AND_PRUNING_AGGREGATIONS.keys():
        raise ValueError(f'Unrecognized aggregation function for pruning with mem opt: {function_name}')
    return MEM_AND_PRUNING_AGGREGATIONS[function_name]


# def get_aggregation_function(
#         function_name: str, is_pruning: bool = False, is_mem_opt: bool = False
# ) -> Union[AggregationFunction, AggregationPruningFunction]:
#     if is_pruning and is_mem_opt:
#         if function_name not in MEM_AND_PRUNING_AGGREGATIONS.keys():
#             raise ValueError(f'Unrecognized aggregation function for pruning with mem opt: {function_name}')
#         return MEM_AND_PRUNING_AGGREGATIONS[function_name]
#
#     if is_pruning:
#         if function_name not in PRUNING_AGGREGATIONS.keys():
#             raise ValueError(f'Unrecognized aggregation function for pruning: {function_name}')
#         return PRUNING_AGGREGATIONS[function_name]
#
#     if is_mem_opt:
#         if function_name not in MEM_AGGREGATIONS.keys():
#             raise ValueError(f'Unrecognized aggregation function for mem optimization: {function_name}')
#         return MEM_AGGREGATIONS[function_name]
#
#     if function_name not in AGGREGATIONS.keys():
#         raise ValueError(f'Unrecognized aggregation function: {function_name}')
#     return AGGREGATIONS[function_name]
