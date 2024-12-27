from input_parser import parse_input
from optimal_subset_with_constraint import calculate_optimal_subset_with_constraint
from utils import calculate_removed_tuples

if __name__ == '__main__':
    aggregation_function, data, aggregation_index, grouped_rows_by_value = parse_input()

    print("The parsed data is: \n", data)
    print("Input aggregation function: ", aggregation_function)

    solution = calculate_optimal_subset_with_constraint(
        grouped_rows_by_value,
        aggregation_function,
        aggregation_index
    )

    print("Optimal solution is: \n", solution)
    print("The removed tuples are: \n", calculate_removed_tuples(data, solution))
