from typing import List

import pandas as pd

from aggregations.aggregation import Aggregation


class Max(Aggregation):
    def get_possible_subsets_aggregations(self, df: pd.DataFrame, agg_col: str) -> List[float]:
        return df[agg_col].unique()

    def aggregate(self, df: pd.DataFrame, agg_col: str) -> float:
        return df[agg_col].max()

    def get_aggregation_packing(self, df: pd.DataFrame, agg_col: str, low: float, high: float) -> pd.DataFrame:
        filtered_df = df.loc[df[agg_col].le(high)]
        max_value = filtered_df[agg_col].max()

        if low > max_value:
            return pd.DataFrame(columns=df.columns)

        return filtered_df

    def __str__(self):
        return "MAX"
