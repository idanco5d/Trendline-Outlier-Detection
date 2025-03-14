# Trendline Outlier Detection

## Description
This code finds an optimal subset of a given set of data, so that a monotonicity constraint is satisfied on the subset.  
Or, more specifically, given a set r, a grouping attribute A, an aggregated attribute B, and an aggregation function f,  
We define b := {r'[B] | r' contained in r, r'[A] = a}, b' := {r'[B] | r' contained in r, r'[A] = a'}.  
The monotonicity constraint the code enforces is for all a and a' in A such that a<=a', then f(b) <= f(b').  

## Usage
Example on how to run the code in the terminal:
```
python main MAX tests/max/input.csv aggregator grouping_1 grouping_2 
```
The needed python version is >=3.11.  
The program takes 4 or more arguments:  
1. The aggregation function you choose. The options are MAX, MIN, COUNT, COUNT_DISTINCT, AVG, SUM, and MEDIAN.
2. A CSV input file path where you hold your data. The data types must be numbers for all your data, except your file's headers.
3. The name of the attribute which you wish to aggregate.
4. The name or names of the attributes you wish to group by. There can be more than one, but there must be at least one.
5. An optional argument for pruning solution, this logic is only available for SUM and AVG. The command is then:
```
python main SUM tests/sum/input.csv aggregator grouping_1 grouping_2 --prune 2
```


In `Tests` folder there are examples for files you can run for all aggregations.