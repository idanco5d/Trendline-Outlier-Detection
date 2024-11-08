from AllowedAggregationFunction import getPossibleSubsetsAggregations
from InputParser import parseInput
from OptimalSubsetWithConstraint import calculateOptimalSubsetWithConstraint, calculateRemovedTuples

if __name__ == '__main__':
    inputFunction, data, aggregationIndex, groupedRowsByValue = parseInput()

    possibleSubsetsAggregations = getPossibleSubsetsAggregations(
        inputFunction, data, aggregationIndex, groupedRowsByValue
    )
    solution = calculateOptimalSubsetWithConstraint(
        inputFunction,
        groupedRowsByValue,
        aggregationIndex,
        possibleSubsetsAggregations
    )

    print("The parsed data is: \n", data)
    print("Input aggregation function: ", inputFunction)
    print("Possible subsets aggregations are: ", possibleSubsetsAggregations)
    print("Optimal solution is: \n", solution)
    print("The removed tuples are: \n", calculateRemovedTuples(data, solution))
