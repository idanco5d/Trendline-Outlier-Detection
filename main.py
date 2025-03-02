from input_parser import parse_input
from optimal_subset_with_constraint import get_optimal_subset

if __name__ == '__main__':
    df, group_cols, agg_col, aggregation = parse_input()

    print("The parsed data is: \n", df)

    subset_df, removed_df = get_optimal_subset(df, group_cols, agg_col, aggregation)
    print(f"Num removed tuples: {len(df)-len(result_df)}/{len(df)}")

    print("Optimal solution is: \n", subset_df)
    print("The removed tuples are: \n", removed_df)

    subset_df.to_csv('subset.csv')
    removed_df.to_csv('removed.csv')
