import unittest

import pandas as pd
from pandas._testing import assert_frame_equal

from AggregationFunctions.AggregationFunction import AggregationFunction
from AggregationFunctions.AverageFunction import AverageFunction
from AggregationFunctions.CountDistinctFunction import CountDistinctFunction
from AggregationFunctions.CountFunction import CountFunction
from AggregationFunctions.MaxFunction import MaxFunction
from AggregationFunctions.MinFunction import MinFunction
from AggregationFunctions.SumFunction import SumFunction
from InputParser import parseCsvToDataFrame, groupFrameByAttributes
from OptimalSubsetWithConstraint import calculateOptimalSubsetWithConstraint


class TestOptimalSolution(unittest.TestCase):
    def testMax(self):
        actualSolution = getOptimalSolution("Max/max_initial_file.csv", MaxFunction())
        expectedSolution = pd.read_csv("Max/max_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testMin(self):
        actualSolution = getOptimalSolution("Min/min_initial_file.csv", MinFunction())
        expectedSolution = pd.read_csv("Min/min_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testCount(self):
        actualSolution = getOptimalSolution("Count/count_initial_file.csv", CountFunction())
        expectedSolution = pd.read_csv("Count/count_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testCountDistinct(self):
        actualSolution = getOptimalSolution("Count_Distinct/count_distinct_initial_file.csv", CountDistinctFunction())
        expectedSolution = pd.read_csv("Count_Distinct/count_distinct_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testSum(self):
        actualSolution = getOptimalSolution("Sum/sum_initial_file.csv", SumFunction())
        expectedSolution = pd.read_csv("Sum/sum_expected_result.csv")
        assertDataFramesEqual(actualSolution, expectedSolution)

    def testAverage(self):
        actualSolution = getOptimalSolution("Average/average_initial_file.csv", AverageFunction())
        expectedSolution = pd.read_csv("Average/average_expected_result.csv")
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
