import argparse
import csv
from typing import List, Dict

from AllowedAggregationFunction import AllowedAggregationFunction


def getInputArguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('aggregationFunction', type=str, help='Chosen aggregation function')
    parser.add_argument('datasetFileName', type=str, help='Your dataset csv file')
    parser.add_argument('conditionAttributeIndex', type=int, help='Index of the condition attribute')
    parser.add_argument('aggregationAttributeIndex', type=int, help='Index of the aggregated attribute')
    return parser.parse_args()


def parseCsvToList(filename: str) -> List[List[int]]:
    with open(filename, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        dataset = [[int(value) for value in row] for row in csv_reader]
    return dataset


def parseInputFunctionName(functionName: str) -> AllowedAggregationFunction:
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
        case _:
            raise ValueError(f"Unrecognized aggregation function: {functionName}")


def getPossibleSubsetsAggregations(
        functionType: AllowedAggregationFunction, dataset: List[List[int]]
) -> List[List[int]]:
    match functionType:
        case AllowedAggregationFunction.MIN:
            return dataset
        case AllowedAggregationFunction.MAX:
            return dataset


def groupByIndexValue(dataset: List[List[int]], index: int) -> Dict[int, List[List[int]]]:
    result = {}

    for row in dataset:
        key = row[index]
        if key not in result:
            result[key] = []
        result[key].append(row)

    return dict(sorted(result.items()))


if __name__ == '__main__':
    args = getInputArguments()

    data = parseCsvToList(args.datasetFileName)
    aggregationFunction = parseInputFunctionName(args.aggregationFunction)

    possibleSubsetsAggregations = getPossibleSubsetsAggregations(aggregationFunction, data)
