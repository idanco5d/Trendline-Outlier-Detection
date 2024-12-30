from typing import List

import pandas as pd

from aggregations.aggregation import Aggregation


class Count(Aggregation):
    def get_possible_subsets_aggregations(self, df: pd.DataFrame, agg_col: str) -> List[float]:
        return list(range(len(df) + 1))

    def aggregate(self, df: pd.DataFrame, agg_col: str) -> float:
        return len(df)

    def get_aggregation_packing(self, df: pd.DataFrame, agg_col: str, low: float, high: float) -> pd.DataFrame:
        size = len(df)

        if size < low:
            return pd.DataFrame(columns=df.columns)

        amount_tuples_to_return = int(min(high, size))
        return df.head(amount_tuples_to_return)

    def __str__(self):
        return "COUNT"
