from input_parser import parse_input, RawArgs
from optimal_subset_with_constraint import get_optimal_subset, get_optimal_subset_mem_opt, get_optimal_subset_pruning_mem_opt
from optimal_subset_with_constraints_pruning import get_optimal_subset_pruning
from optimal_subset_with_constraint_unified import get_optimal_subset_F_first
import os
import time
import tracemalloc

def run_algorithm(initial_args: RawArgs | None = None) -> tuple:
    input_data = parse_input(initial_args)
    print("The parsed data is: \n", input_data.df)
    hprune_str = 'hprune' if input_data.prune_h else 'noprune'
    subset_df, removed_df = get_optimal_subset_F_first(
        df=input_data.df,
        group_cols=input_data.group_cols,
        agg_col=input_data.agg_col,
        Agg=input_data.aggregation,
        max_removed=input_data.prune_aggpack_by_greedy,
        prune_dp_by_max_removed=input_data.prune_dp_by_greedy,
        prune_h=input_data.prune_h,
        time_cutoff_seconds=input_data.time_cutoff_seconds,
        htrack_file=os.path.join(input_data.output_folder,
                                 f"htrack-{hprune_str}-{input_data.orig_fname}-{input_data.agg_name}.txt")
    )
    return input_data, subset_df, removed_df

if __name__ == '__main__':
    s = time.time()
    tracemalloc.start()

    input_data, subset_df, removed_df = run_algorithm()

    # if input_data.prune_aggpack_by_greedy is not None and input_data.mem_opt:
    #     if input_data.agg_name != 'MEDIAN':
    #         subset_df, removed_df = get_optimal_subset_pruning_mem_opt(
    #             df=input_data.df,
    #             group_cols=input_data.group_cols,
    #             agg_col=input_data.agg_col,
    #             Agg=input_data.aggregation,
    #             max_removed=input_data.prune,
    #             time_cutoff_seconds=input_data.time_cutoff_seconds,
    #             parallelize=True,
    #         )
    #     else:
    #         subset_df, removed_df = get_optimal_subset_F_first(
    #             df=input_data.df,
    #             group_cols=input_data.group_cols,
    #             agg_col=input_data.agg_col,
    #             Agg=input_data.aggregation,
    #             # time_cutoff_seconds=input_data.time_cutoff_seconds,
    #             max_removed=input_data.prune_aggpack_by_greedy,
    #             prune_dp_by_max_removed=input_data.prune_dp_by_greedy,
    #             prune_h=input_data.prune_h
    #         )
    #
    # elif input_data.prune is not None:
    #     subset_df, removed_df = get_optimal_subset_pruning(
    #         df=input_data.df,
    #         group_cols=input_data.group_cols,
    #         agg_col=input_data.agg_col,
    #         agg=input_data.aggregation,
    #         max_removed=input_data.prune,
    #         parallelize=False,
    #     )
    # elif input_data.mem_opt:
    #     #subset_df, removed_df = get_optimal_subset_mem_opt(
    #     subset_df, removed_df = get_optimal_subset_F_first(
    #         df=input_data.df,
    #         group_cols=input_data.group_cols,
    #         agg_col=input_data.agg_col,
    #         Agg=input_data.aggregation,
    #         #time_cutoff_seconds=input_data.time_cutoff_seconds,
    #     )
    # else:
    #     subset_df, removed_df = get_optimal_subset(
    #         df=input_data.df,
    #         group_cols=input_data.group_cols,
    #         agg_col=input_data.agg_col,
    #         agg=input_data.aggregation
    #     )

    print(f"maximal memory usage: {tracemalloc.get_traced_memory()[1]}")
    tracemalloc.stop()
    print(f"Num removed tuples: {len(removed_df)}/{len(input_data.df)}")
    print(f"time: {time.time() - s}")
    # print("Optimal solution is: \n", subset_df)
    print("The removed tuples are: \n", removed_df[input_data.group_cols].value_counts())

    #subset_df.to_csv(os.path.join(args.output_folder, f"dp_result-{input_data.orig_fname}.csv"), index=True)
    removed_df.to_csv(os.path.join(input_data.output_folder, f"dp_removed-{input_data.orig_fname}-{input_data.agg_name}.csv"), index=True)
    # print(f"time: {time.time()-s}")
