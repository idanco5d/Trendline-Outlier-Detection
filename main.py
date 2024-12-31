from input_parser import parse_input
from optimal_subset_with_constraint import get_optimal_subset
from utils import calculate_removed_tuples

if __name__ == '__main__':
    df, group_cols, agg_col, aggregation = parse_input()

    print("The parsed data is: \n", df)

    result_df = get_optimal_subset(df, group_cols, agg_col, aggregation)

    print("Optimal solution is: \n", result_df)
    print("The removed tuples are: \n", calculate_removed_tuples(df, result_df))
