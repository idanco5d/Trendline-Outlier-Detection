import unittest

import pandas as pd
from pandas._testing import assert_frame_equal

from aggregation_functions.aggregation_function import AggregationFunction
from aggregation_functions.average_function import AverageFunction
from aggregation_functions.count_distinct_function import CountDistinctFunction
from aggregation_functions.count_function import CountFunction
from aggregation_functions.max_function import MaxFunction
from aggregation_functions.min_function import MinFunction
from aggregation_functions.sum_function import SumFunction
from input_parser import parseCsvToDataFrame, groupFrameByAttributes
from optimal_subset_with_constraint import calculateOptimalSubsetWithConstraint


class TestOptimalSolution(unittest.TestCase):
    def testMax(self):
        actualSolution = getOptimalSolution("max/max_initial_file.csv", MaxFunction())
        expectedSolution = pd.read_csv("max/max_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testMin(self):
        actualSolution = getOptimalSolution("min/min_initial_file.csv", MinFunction())
        expectedSolution = pd.read_csv("min/min_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testCount(self):
        actualSolution = getOptimalSolution("count/count_initial_file.csv", CountFunction())
        expectedSolution = pd.read_csv("count/count_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testCountDistinct(self):
        actualSolution = getOptimalSolution("count_distinct/count_distinct_initial_file.csv", CountDistinctFunction())
        expectedSolution = pd.read_csv("count_distinct/count_distinct_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testSum(self):
        actualSolution = getOptimalSolution("sum/sum_initial_file.csv", SumFunction())
        expectedSolution = pd.read_csv("sum/sum_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testAverage(self):
        actualSolution = getOptimalSolution("average/average_initial_file.csv", AverageFunction())
        expectedSolution = pd.read_csv("average/average_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)


def getOptimalSolution(inputFileName: str, function: AggregationFunction) -> pd.DataFrame:
    data = parseCsvToDataFrame(inputFileName)
    groupedRowsByValue = groupFrameByAttributes(data, ['grouping_1', 'grouping_2'], "aggregator")
    return calculateOptimalSubsetWithConstraint(
        groupedRowsByValue,
        function,
        3
    )


def assertDataFramesEqual(df1: pd.DataFrame, df2: pd.DataFrame):
    assert_frame_equal(resetDfIndex(df1), resetDfIndex(df2), False)


def resetDfIndex(df: pd.DataFrame) -> pd.DataFrame:
    return df.reset_index(drop=True)


if __name__ == '__main__':
    unittest.main()
