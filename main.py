from InputParser import parseInput
from OptimalSubsetWithConstraint import calculateOptimalSubsetWithConstraint
from Utils import calculateRemovedTuples

if __name__ == '__main__':
    aggregationFunction, data, aggregationIndex, groupedRowsByValue = parseInput()

    possibleSubsetsAggregations = aggregationFunction.getPossibleSubsetsAggregations(
        data, aggregationIndex, groupedRowsByValue
    )

    solution = calculateOptimalSubsetWithConstraint(
        groupedRowsByValue,
        aggregationFunction,
        possibleSubsetsAggregations,
        aggregationIndex
    )

    print("The parsed data is: \n", data)
    print("Input aggregation function: ", aggregationFunction)
    print("Possible subsets aggregations are: ", possibleSubsetsAggregations)
    print("Optimal solution is: \n", solution)
    print("The removed tuples are: \n", calculateRemovedTuples(data, solution))
