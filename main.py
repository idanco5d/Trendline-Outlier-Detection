from input_parser import parse_input
from optimal_subset_with_constraint import calculate_optimal_subset_with_constraint
from utils import calculate_removed_tuples

if __name__ == '__main__':
    agg, data, agg_col, grouped_rows_by_value = parse_input()

    print("The parsed data is: \n", data)
    print("Input aggregation function: ", agg)

    solution = calculate_optimal_subset_with_constraint(
        grouped_rows_by_value,
        agg,
        agg_col
    )

    print("Optimal solution is: \n", solution)
    print("The removed tuples are: \n", calculate_removed_tuples(data, solution))
