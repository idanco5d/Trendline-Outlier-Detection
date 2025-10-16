import os
from dataclasses import asdict

import pandas as pd

from background import my_celery_app
from input_parser import parse_input
from optimal_subset_with_constraint_unified import get_optimal_subset_F_first
from class_models import RawArgs
from pubsub import get_redis_sync_client, redis_sync_client_cleanup


def _run_algorithm(initial_args: RawArgs | None = None, **kwargs) -> tuple:
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
                                 f"htrack-{hprune_str}-{input_data.orig_fname}-{input_data.agg_name}.txt"),
        **kwargs
    )
    return input_data, subset_df, removed_df


def run_algorithm(initial_args: RawArgs | None = None) -> tuple:
    return _run_algorithm(initial_args)


@my_celery_app.task(bind=True)
def run_algorithm_background(self, initial_args: dict | None = None) -> tuple:
    task_id = self.request.id
    if initial_args is not None:
        initial_args = RawArgs(**initial_args)
    redis_client = get_redis_sync_client()
    input_data, subset_df, removed_df = _run_algorithm(
        initial_args,
        task_id=task_id,
        notify=True,
        redis_client=redis_client)

    input_data_dict = asdict(input_data)

    for key, value in input_data_dict.items():
        if isinstance(value, pd.DataFrame):
            input_data_dict[key] = value.to_dict('records')
        elif isinstance(value, type):
            input_data_dict[key] = f"<type:{value.__name__}>"

    redis_sync_client_cleanup(client=redis_client)

    return input_data_dict, subset_df.to_dict('records'), removed_df.to_dict('records')