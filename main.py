from aggregations_pruning import get_sum_subsets_pruning
from input_parser import parse_input
from optimal_subset_with_constraint import get_optimal_subset
from optimal_subset_with_constraints_pruning import get_optimal_subset_pruning

if __name__ == '__main__':
    df, group_cols, agg_col, aggregation = parse_input()
    print("The parsed data is: \n", df)

    if aggregation == get_sum_subsets_pruning:
        max_removed = int(input('Max removed tuples:'))
        subset_df, removed_df = get_optimal_subset_pruning(
            df, group_cols, agg_col, aggregation, max_removed
        )
    else:
        subset_df, removed_df = get_optimal_subset(df, group_cols, agg_col, aggregation)

    print(f"Num removed tuples: {len(removed_df)}/{len(df)}")

    print("Optimal solution is: \n", subset_df)
    print("The removed tuples are: \n", removed_df)

    subset_df.to_csv('subset.csv')
    removed_df.to_csv('removed.csv')
