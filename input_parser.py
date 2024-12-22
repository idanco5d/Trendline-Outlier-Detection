import argparse
from typing import List

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from aggregation_functions.aggregation_function import AggregationFunction
from aggregation_functions.average_function import AverageFunction
from aggregation_functions.count_distinct_function import CountDistinctFunction
from aggregation_functions.count_function import CountFunction
from aggregation_functions.max_function import MaxFunction
from aggregation_functions.min_function import MinFunction
from aggregation_functions.sum_function import SumFunction


def parse_input() -> (AggregationFunction, pd.DataFrame, int, DataFrameGroupBy):
    args = get_input_arguments()
    data = parse_csv_to_data_frame(args.datasetFileName)

    return (get_aggregation_function_from_input(args.aggregationFunction),
            data,
            get_aggregation_attribute_index_by_name(data, args.aggregationAttributeName),
            group_frame_by_attributes(data, args.groupingAttributesNames, args.aggregationAttributeName))


def get_input_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('aggregationFunction', type=str, help='Chosen aggregation function')
    parser.add_argument('datasetFileName', type=str, help='Your dataset csv file')
    parser.add_argument('aggregationAttributeName', type=str, help='Name of the aggregated attribute')
    parser.add_argument('groupingAttributesNames', nargs='+', type=str, help='Names of the grouping attributes')

    return parser.parse_args()


def parse_csv_to_data_frame(filename: str) -> pd.DataFrame:
    return pd.read_csv(filename)


def get_aggregation_function_from_input(function_name: str) -> AggregationFunction:
    match function_name:
        case "MAX":
            return MaxFunction()
        case "MIN":
            return MinFunction()
        case "COUNT":
            return CountFunction()
        case "COUNT_DISTINCT":
            return CountDistinctFunction()
        case "SUM":
            return SumFunction()
        case "AVG":
            return AverageFunction()

    raise ValueError(f"Unrecognized aggregation function: {function_name}")


def get_aggregation_attribute_index_by_name(data: pd.DataFrame, aggregation_attribute_name: str) -> int:
    try:
        aggregation_attribute_index = data.columns.get_loc(aggregation_attribute_name)
    except KeyError:
        raise ValueError('Invalid aggregation attribute name')

    return aggregation_attribute_index


def group_frame_by_attributes(data: pd.DataFrame, grouping_attributes_names: List[str],
                              aggregation_attribute_name: str) -> DataFrameGroupBy:
    data = data.sort_values(by=grouping_attributes_names + [aggregation_attribute_name])
    try:
        grouped_rows_by_value = data.groupby(grouping_attributes_names)
    except KeyError:
        raise ValueError('Invalid grouping attribute name')

    return grouped_rows_by_value
