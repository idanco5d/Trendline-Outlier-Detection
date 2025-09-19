import os
from dataclasses import asdict

import pandas as pd

from background import my_celery_app
from input_parser import parse_input, RawArgs
from optimal_subset_with_constraint_unified import get_optimal_subset_F_first


def _run_algorithm(initial_args: RawArgs | None = None) -> tuple:
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


def run_algorithm(initial_args: RawArgs | None = None) -> tuple:
    return _run_algorithm(initial_args)


@my_celery_app.task
def run_algorithm_background(initial_args: dict | None = None) -> tuple:
    if initial_args is not None:
        initial_args = RawArgs(**initial_args)
    input_data, subset_df, removed_df = _run_algorithm(initial_args)

    input_data_dict = asdict(input_data)

    for key, value in input_data_dict.items():
        if isinstance(value, pd.DataFrame):
            input_data_dict[key] = value.to_dict('records')
        elif isinstance(value, type):
            input_data_dict[key] = f"<type:{value.__name__}>"

    return input_data_dict, subset_df.to_dict('records'), removed_df.to_dict('records')