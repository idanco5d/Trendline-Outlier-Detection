import argparse
import csv

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from AllowedAggregationFunction import AllowedAggregationFunction


def getParsedInput() -> (AllowedAggregationFunction, pd.DataFrame, int, DataFrameGroupBy):
    args = getInputArguments()
    data = parseCsvToDataFrame(args.datasetFileName)
    aggregationFunction = getInputFunctionName(args.aggregationFunction)
    groupedRowsByValue = data.groupby(args.groupingAttributeName)

    return aggregationFunction, data, args.aggregationAttributeIndex, groupedRowsByValue


def getInputArguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('aggregationFunction', type=str, help='Chosen aggregation function')
    parser.add_argument('datasetFileName', type=str, help='Your dataset csv file')
    parser.add_argument('groupingAttributeName', type=str, help='Name of the grouping attribute')
    parser.add_argument('aggregationAttributeIndex', type=int, help='Name of the aggregated attribute')
    return parser.parse_args()


def parseCsvToDataFrame(filename: str) -> pd.DataFrame:
    with open(filename, mode='r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        dataset = [[int(value) for value in row] for row in csv_reader]

    return pd.DataFrame(dataset, columns=header, index=range(len(dataset)))


def getInputFunctionName(functionName: str) -> AllowedAggregationFunction:
    match functionName:
        case "MAX":
            return AllowedAggregationFunction.MAX
        case "MIN":
            return AllowedAggregationFunction.MIN
        case "COUNT":
            return AllowedAggregationFunction.COUNT
        case "COUNT_DISTINCT":
            return AllowedAggregationFunction.COUNT_DISTINCT
        case "SUM":
            return AllowedAggregationFunction.SUM
        case "AVG":
            return AllowedAggregationFunction.AVG

    raise ValueError(f"Unrecognized aggregation function: {functionName}")
