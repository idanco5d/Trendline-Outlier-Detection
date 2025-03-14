from input_parser import parse_input
from optimal_subset_with_constraint import get_optimal_subset
from optimal_subset_with_constraints_pruning import get_optimal_subset_pruning

if __name__ == '__main__':
    input_data = parse_input()
    print("The parsed data is: \n", input_data.df)

    if input_data.prune is not None:
        subset_df, removed_df = get_optimal_subset_pruning(
            df=input_data.df,
            group_cols=input_data.group_cols,
            agg_col=input_data.agg_col,
            agg=input_data.aggregation,
            max_removed=input_data.prune
        )
    else:
        subset_df, removed_df = get_optimal_subset(
            df=input_data.df,
            group_cols=input_data.group_cols,
            agg_col=input_data.agg_col,
            agg=input_data.aggregation
        )

    print(f"Num removed tuples: {len(removed_df)}/{len(input_data.df)}")

    print("Optimal solution is: \n", subset_df)
    print("The removed tuples are: \n", removed_df)

    subset_df.to_csv('subset.csv')
    removed_df.to_csv('removed.csv')
