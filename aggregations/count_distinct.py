from typing import List

import pandas as pd

from aggregations.aggregation import Aggregation


class CountDistinct(Aggregation):
    def get_possible_subsets_aggregations(self, df: pd.DataFrame, agg_col: str) -> List[float]:
        return list(range(df[agg_col].nunique() + 1))

    def aggregate(self, df: pd.DataFrame, agg_col: str) -> float:
        return df[agg_col].nunique()

    def get_aggregation_packing(self, df: pd.DataFrame, agg_col: str, low: float, high: float) -> pd.DataFrame:
        count_distinct = df[agg_col].nunique()

        if count_distinct < low:
            return pd.DataFrame(columns=df.columns)

        num_to_remove = count_distinct - high
        value_counts = df[agg_col].value_counts()
        # If num_to_remove <= 0, nsmallest returns an empty list
        least_common = value_counts.nsmallest(num_to_remove).index
        return df.loc[~df[agg_col].isin(least_common)]

    def __str__(self):
        return "COUNT_DISTINCT"
