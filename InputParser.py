import argparse
import csv
from typing import List

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AggregationFunctions.AggregationFunction import AggregationFunction
from AggregationFunctions.AverageFunction import AverageFunction
from AggregationFunctions.CountDistinctFunction import CountDistinctFunction
from AggregationFunctions.CountFunction import CountFunction
from AggregationFunctions.MaxFunction import MaxFunction
from AggregationFunctions.MinFunction import MinFunction
from AggregationFunctions.SumFunction import SumFunction


def parseInput() -> (AggregationFunction, pd.DataFrame, int, DataFrameGroupBy):
    args = getInputArguments()
    data = parseCsvToDataFrame(args.datasetFileName)

    return (getAggregationFunctionFromInput(args.aggregationFunction),
            data,
            getAggregationAttributeIndexByName(data, args.aggregationAttributeName),
            groupFrameByAttributes(data, args.groupingAttributesNames))


def getInputArguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('aggregationFunction', type=str, help='Chosen aggregation function')
    parser.add_argument('datasetFileName', type=str, help='Your dataset csv file')
    parser.add_argument('aggregationAttributeName', type=str, help='Name of the aggregated attribute')
    parser.add_argument('groupingAttributesNames', nargs='+', type=str, help='Names of the grouping attributes')

    return parser.parse_args()


def parseCsvToDataFrame(filename: str) -> pd.DataFrame:
    with open(filename, mode='r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        dataset = [[int(value) for value in row] for row in csv_reader]

    return pd.DataFrame(dataset, columns=header, index=range(len(dataset)))


def getAggregationFunctionFromInput(functionName: str) -> AggregationFunction:
    match functionName:
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

    raise ValueError(f"Unrecognized aggregation function: {functionName}")


def getAggregationAttributeIndexByName(data: pd.DataFrame, aggregationAttributeName: str) -> int:
    try:
        aggregationAttributeIndex = data.columns.get_loc(aggregationAttributeName)
    except KeyError:
        raise ValueError('Invalid aggregation attribute name')

    return aggregationAttributeIndex


def groupFrameByAttributes(data: pd.DataFrame, groupingAttributesNames: List[str]) -> DataFrameGroupBy:
    try:
        groupedRowsByValue = data.groupby(groupingAttributesNames)
    except KeyError:
        raise ValueError('Invalid grouping attribute name')

    return groupedRowsByValue
