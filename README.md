# Trendline Outlier Detection

## Description
This code finds an optimal subset of a given set of data, so that a monotonicity constraint is satisfied on the subset.  
Or, more specifically, given a set r, a grouping attribute A, an aggregated attribute B, and an aggregation function f,  
We define b := {r'[B] | r' contained in r, r'[A] = a}, b' := {r'[B] | r' contained in r, r'[A] = a'}.  
The monotonicity constraint the code enforces is for all a and a' in A such that a<=a', then f(b) <= f(b').  

## Usage
The needed python version is >=3.11.  
The program takes 4 or more arguments:  
1. The aggregation function you choose. The options are MAX, MIN, COUNT, COUNT_DISTINCT, AVG (average) and SUM.
2. A CSV input file path where you hold your data. The data types must be numbers for all your data, except your file's headers.
3. The name of the attribute which you wish to aggregate.
4. The name or names of the attributes you wish to group by. There can be more than one, but there must be at least one.  

Future possible directions:
1. To better reassess the possible aggregations: If the same data exists for the different groups, we already calculated their possible aggregations (function: AggregationFunction.getPossibleSubsetsAggregations)
2. Try to change the Binary Search Default Dict to sort the values only once when we finish going through specific r_i
3. Cache the aggregation packings for MAX, MIN, COUNT, COUNT_DISTINCT  
4. Parallel computing: Possibly for a single r_i  
5. Remove the non aggregated and grouped columns from the data frame at the beginning when the input is parsed in `parseInput`, and add them back at the end after the ideal solution is found  

Example on how to run the code in the terminal: "**python main.py MAX my_csv_file.csv aggregation_column grouping_column**"  

In `Tests` folder there are examples for files you can run:  
For example, `Tests/Max/max_initial_file.csv` which has two grouping attributes (`grouping_1`, `grouping_2`), an aggregation attribute (`aggregator`) and a column which has nothing to do with the calculations (`no_meaning`).