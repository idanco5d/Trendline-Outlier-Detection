import argparse
import csv
from typing import List, Dict, Set

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
        functionType: AllowedAggregationFunction, dataset: List[List[int]], aggregationIndex: int
) -> List[int]:
    match functionType:
        case AllowedAggregationFunction.MIN:
            return [row[aggregationIndex] for row in dataset]
        case AllowedAggregationFunction.MAX:
            return [row[aggregationIndex] for row in dataset]


def groupByIndexValue(dataset: List[List[int]], index: int) -> Dict[int, List[List[int]]]:
    result = {}

    for row in dataset:
        key = row[index]
        if key not in result:
            result[key] = []
        result[key].append(row)

    return dict(sorted(result.items()))


def maxBoundedAggregation(
        dataset: List[List[int]], aggregationAttributeIndex: int, lowerBound: int, upperBound: int
) -> Set[int]:
    result = set()
    for datasetTuple in dataset:
        if lowerBound <= datasetTuple[aggregationAttributeIndex] <= upperBound:
            result.add(datasetTuple[aggregationAttributeIndex])
    return result


def optimalSolution(
        groupedRows: Dict[int, List[List[int]]],
        aggregationAttributeIndex: int,
        subsetsAggregations: List[int],
        boundary: int
) -> Set[int]:
    solution = maxBoundedAggregation(
        next(iter(groupedRows.values())), aggregationAttributeIndex, min(subsetsAggregations), boundary
    )
    for key in groupedRows:
        for upperBound in subsetsAggregations:
            solution = solution.union(
                maxBoundedAggregation(groupedRows[key], aggregationAttributeIndex, upperBound, boundary)
            )
    return solution


if __name__ == '__main__':
    args = getInputArguments()

    data = parseCsvToList(args.datasetFileName)
    aggregationFunction = parseInputFunctionName(args.aggregationFunction)

    possibleSubsetsAggregations = getPossibleSubsetsAggregations(
        aggregationFunction, data, args.aggregationAttributeIndex
    )
    groupedRowsByValue = groupByIndexValue(data, args.conditionAttributeIndex)

    print("Data is: ", data)
    print("Aggregation function: ", aggregationFunction)
    print("Possible subsets is: ", possibleSubsetsAggregations)
    print("Grouped rows is: ", groupedRowsByValue)
    print(
        "Optimal solution is: ",
        optimalSolution(
            groupedRowsByValue,
            args.aggregationAttributeIndex,
            possibleSubsetsAggregations,
            max(possibleSubsetsAggregations)
        )
    )
