# SPARK BY KEY OPERATIONS

In this we'll cover 4 ByKey operations in Spark and all these transformations operates on Key Value Pairs(PairRDD).

	1. groupByKey		Lazy, Wide, Costly(Doesn't use combiner), Hash Partition
	2. reduceByKey		Lazy, Wide, Optimized, Input & Output DataType should be same
	3. aggregateByKey	Lazy, Wide, Optimizied, Expects Initial Value, Input & Output DataType can be different
	4. combineByKey		Lazy, Wide, Optimized, Expectes the Initial Function, Input & Output DataType can be different

## groupByKey

Spark RDD groupByKey function collects the values for each key in a form of an iterator. groupByKey function groups all values with respect to a single key data in the partitions and shuffled over the network to form a key and list of values.

	groupByKey()		- groups the values for each key in the RDD into a single sequence
	groupByKey(numTasks) 	- takes the number as an argument for tasks to execute to generate Output RDD
	groupByKey(partitioner)	- takes the partitioner function as an argument for creating partitions in output RDD

<img src="../Screenshots/GroupByKeyExample.jpg">

SYNTAX:

`pairRDD.groupByKey()`

EXAMPLE:

`pairRDD.groupByKey()`

## reduceByKey

Spark RDD reduceByKey function merges the values for each key using an associative reduce function (it should be Commutative and Associative in mathematical nature).
When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function. The function should be able to take arguments of same type and it returns same result data type. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.

	reduceByKey(function)			- this produces hash-partitioned output with existing number of partitions / tasks
	reduceByKey(function, [numTasks])	- this produces hash-partitioned output with the given number of partitions / tasks
	reduceByKey(partitioner, function)	- this produces the output using the given Partitioner and the Reducer function.

<img src="../Screenshots/ReduceByKeyExample.jpg">

SYNTAX:

`pairRDD.reduceByKey(reduceFunction)`

EXAMPLE:

```
pairRDD.reduceByKey((x, y) => x + y)
or
pairRDD.reduceByKey(_ + _)
```

## aggregateByKey

Spark aggregateByKey function aggregates the values of each key, using given combine functions and a neutral “zero value”. The aggregateByKey function aggregates values for each key and and returns a different type of value for that key. aggregateByKey function in Spark accepts total 3 parameters,

	  i. Initial value or Zero value
		- It can be 0 if aggregation is type of sum of all values
		- It can be Double.MaxValue if aggregation objective is to find minimum value
		- It can be Double.MinValue value if aggregation objective is to find maximum value
		- It can be an empty List or Map object, if we just want a respective collection as an output for each key
	 ii. Sequence operation function which transforms/merges data of one type [V] to another type [U]
	iii. Combination operation function which merges multiple transformed type [U] to a single type 

<img src="../Screenshots/AggregateByKeyExample.jpg">

SYNTAX:

`pairRDD.aggregateByKey(InitalValue)(SequenceFunction, MergeFunction)`

EXAMPLE:

```
pairRDD.aggregateByKey(
  (0, 0)				 // InitalValue
  )(
    (x, y) => (x._1 + y, x._2 + 1), 	 // Sequence / MergeValue Function
    (x, y) => (x._1 + y._1, x._2 + y._2) // Merge Combiners Function
  )
```

## combineByKey

Spark combineByKey RDD transformation is very similar to combiner in Hadoop MapReduce programming. combineByKey transform any PairRDD[(K,V)] to the RDD[(K,C)] where C is the result of any aggregation of all values under key K.

Spark combineByKey takes 3 functions as arguments:

	createCombiner
	mergeValue
	mergeCombiners

#####   i. createCombiner function:
	It is a first aggregation step for each key
	It will be executed when any new key is found in a partition
	Execution of this lambda function is local to a partition of a node, on each individual values

#####  ii. mergeValue
	This function executes when next subsequent value is given to combiner
	It also executes locally on each partition of a node and combines all values
	Arguments of this function are a accumulator and a new value
	It combines a new value in existing accumulator

##### iii. mergeCombiner
	This is the final function used to merge two accumulators (i.e. combiners) of a single key across the partitions to generate final expected result
	Arguments are two accumulators (i.e. combiners)
	Merge results of a single key from different partitions

<img src="../Screenshots/CombineByKeyExample.jpg">

SYNTAX:

`pairRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)`

EXAMPLE:

```
pairRDD.combineByKey(
    (x: Int) => (0, 0)							// Create Combiner Function
    (x: Int, y: Int) => (x._1 + y, x._2 + 1), 				// Merge Value Function
    (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)	// Merge Combiners Function
  )
```
