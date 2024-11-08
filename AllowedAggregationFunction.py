from enum import Enum
from typing import Set

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


# TODO create an abstract class aggregation function with the case functions
# TODO add support for multiple grouping attributes
# TODO add custom functions for sum and avg

class AllowedAggregationFunction(Enum):
    MAX, MIN, COUNT, COUNT_DISTINCT, SUM, AVG = range(6)


def getPossibleSubsetsAggregations(
        functionType: AllowedAggregationFunction,
        dataFrame: pd.DataFrame,
        aggregateIndex: int,
        groupedRows: DataFrameGroupBy
) -> Set[int]:
    match functionType:
        case AllowedAggregationFunction.MAX:
            return set(dataFrame.iloc[:, aggregateIndex])
        case AllowedAggregationFunction.MIN:
            return set(dataFrame.iloc[:, aggregateIndex])
        case AllowedAggregationFunction.COUNT:
            groupsSizes = groupedRows.size()
            return set(range(groupsSizes.max() + 1))
        case AllowedAggregationFunction.COUNT_DISTINCT:
            return set(range(len(dataFrame.iloc[:, aggregateIndex].unique()) + 1))
        case _:
            raise ValueError(f'Unsupported aggregation function: {functionType}')


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
            return aggregationColumn.nunique()
        case AllowedAggregationFunction.SUM:
            return sum(aggregationColumn)
        case AllowedAggregationFunction.AVG:
            return sum(aggregationColumn) / len(aggregationColumn)

    raise ValueError(f'Unexpected aggregation function: {aggregationFunction}')
