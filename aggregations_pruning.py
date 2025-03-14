from typing import Dict

import pandas as pd

from aggregations import get_index_set


def get_sum_subsets_pruning(df: pd.DataFrame, agg_col: str, min_subset_size: int = None) -> Dict[float, set]:
    # Dynamic programming dictionary: key = sum, value = indices of subset with sum & maximal size
    sum_subsets = {df[agg_col].sum(): get_index_set(df)}

    for index, row in df.iterrows():
        value = row[agg_col]
        current_sum_subsets = sum_subsets.copy()  # Avoid modifying dict while iterating

        for current_sum, subset in sum_subsets.items():
            if min_subset_size is not None and len(subset) <= min_subset_size:
                continue
            if index not in subset:
                continue
            new_sum = current_sum - value
            if new_sum not in current_sum_subsets or len(sum_subsets[new_sum]) < len(subset) - 1:
                current_sum_subsets[new_sum] = subset - {index}

        sum_subsets = current_sum_subsets

    return sum_subsets
