import os
import time
import tracemalloc

from algorithm import run_algorithm


if __name__ == '__main__':
    s = time.time()
    tracemalloc.start()

    input_data, subset_df, removed_df = run_algorithm()

    print(f"maximal memory usage: {tracemalloc.get_traced_memory()[1]}")
    tracemalloc.stop()
    print(f"Num removed tuples: {len(removed_df)}/{len(input_data.df)}")
    print(f"time: {time.time() - s}")
    print("The removed tuples are: \n", removed_df[input_data.group_cols].value_counts())

    if input_data.output_folder != '':  # [Carmel] I should've marked this field as of "None" type
        # but I don't want to mess with the 'Input' dataclass at the moment...
        removed_df.to_csv(os.path.join(input_data.output_folder, f"dp_removed-{input_data.orig_fname}-{input_data.agg_name}.csv"), index=True)