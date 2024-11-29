from InputParser import parseInput
from OptimalSubsetWithConstraint import calculateOptimalSubsetWithConstraint
from Utils import calculateRemovedTuples

if __name__ == '__main__':
    aggregationFunction, data, aggregationIndex, groupedRowsByValue = parseInput()

    possibleSubsetsAggregations = aggregationFunction.getPossibleSubsetsAggregations(
        data, aggregationIndex, groupedRowsByValue
    )
    print("The parsed data is: \n", data)
    print("Input aggregation function: ", aggregationFunction)

    solution = calculateOptimalSubsetWithConstraint(
        groupedRowsByValue,
        aggregationFunction,
        possibleSubsetsAggregations,
        aggregationIndex
    )

    print("Optimal solution is: \n", solution)
    print("The removed tuples are: \n", calculateRemovedTuples(data, solution))
