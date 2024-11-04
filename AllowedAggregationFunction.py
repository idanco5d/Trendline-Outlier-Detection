from enum import Enum

import pandas as pd


class AllowedAggregationFunction(Enum):
    MAX, MIN, COUNT, COUNT_DISTINCT, SUM, AVG = range(6)


def aggregate(
        aggregationFunction: AllowedAggregationFunction,
        dataFrame: pd.DataFrame,
        aggregationAttributeIndex: int
) -> int:
    aggregationColumn = dataFrame.iloc[:, aggregationAttributeIndex]

    match aggregationFunction:
        case AllowedAggregationFunction.MAX:
            return max(aggregationColumn)
        case AllowedAggregationFunction.MIN:
            return min(aggregationColumn)
        case AllowedAggregationFunction.COUNT:
            return len(aggregationColumn)
        case AllowedAggregationFunction.COUNT_DISTINCT:
            return len(aggregationColumn.unique())
        case AllowedAggregationFunction.SUM:
            return sum(aggregationColumn)
        case AllowedAggregationFunction.AVG:
            return sum(aggregationColumn) / len(aggregationColumn)

    raise ValueError(f'Unexpected aggregation function: {aggregationFunction}')
