from input_parser import parse_input
from optimal_subset_with_constraint import get_optimal_subset, get_optimal_subset_mem_opt, get_optimal_subset_pruning_mem_opt
from optimal_subset_with_constraints_pruning import get_optimal_subset_pruning
import os
import time

if __name__ == '__main__':
    s = time.time()
    input_data = parse_input()
    print("The parsed data is: \n", input_data.df)
    
    if input_data.prune is not None and input_data.mem_opt:
        subset_df, removed_df = get_optimal_subset_pruning_mem_opt(
            df=input_data.df,
            group_cols=input_data.group_cols,
            agg_col=input_data.agg_col,
            Agg=input_data.aggregation,
            max_removed=input_data.prune,
            time_cutoff_seconds=input_data.time_cutoff_seconds,
            parallelize=False,
        )
    elif input_data.prune is not None:
        subset_df, removed_df = get_optimal_subset_pruning(
            df=input_data.df,
            group_cols=input_data.group_cols,
            agg_col=input_data.agg_col,
            agg=input_data.aggregation,
            max_removed=input_data.prune
        )
    elif input_data.mem_opt:
        subset_df, removed_df = get_optimal_subset_mem_opt(
            df=input_data.df,
            group_cols=input_data.group_cols,
            agg_col=input_data.agg_col,
            Agg=input_data.aggregation,
            time_cutoff_seconds=input_data.time_cutoff_seconds,
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

    #subset_df.to_csv(os.path.join(args.output_folder, f"dp_result-{input_data.orig_fname}.csv"), index=True)
    removed_df.to_csv(os.path.join(input_data.output_folder, f"dp_removed-{input_data.orig_fname}.csv"), index=True)
    print(f"time: {time.time()-s}")
