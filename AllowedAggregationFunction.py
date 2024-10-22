from enum import Enum


class AllowedAggregationFunction(Enum):
    MAX, MIN, COUNT, COUNT_DISTINCT, SUM, AVG = range(6)
