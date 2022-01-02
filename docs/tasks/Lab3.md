#### TASK #1 - EXECUTORS
In this task, you will implement nine executors, one for each of the following operations: sequential scan, insert, update, delete, nested loop join, hash join, aggregation, limit, and distinct. For each query plan operator type, there is a corresponding executor object that implements the Init and Next methods. The Init method initializes the internal state of the operator (e.g., retrieving the corresponding table to scan). The Next method provides the iterator interface that returns a tuple and corresponding RID on each invocation (or an indicator that the executor is exhausted).

The executors that you will implement are defined in the following header files:
```shell
src/include/execution/executors/seq_scan_executor.h
src/include/execution/executors/insert_executor.h
src/include/execution/executors/update_executor.h
src/include/execution/executors/delete_executor.h
src/include/execution/executors/nested_loop_join_executor.h
src/include/execution/executors/hash_join_executor.h
src/include/execution/executors/aggregation_executor.h
src/include/execution/executors/limit_executor.h
src/include/execution/executors/distinct_executor.h
```
Headers (.h) and implementation files (.cpp) for other executors exist in the repository (e.g. index_scan_executor and nested_index_join_executor). You are not responsible for implementing these executors.

Each executor is responsible for processing a single plan node type. Plan nodes are the individual elements that compose a query plan. Each plan node may define information specific to the operator that it represents. For instance, the plan node for a sequential scan must define the identifier for the table over which the scan is performed, while the plan node for the LIMIT operator does not require this information. The plan nodes for the executors that you will implement are declared in the following header files:
```shell
src/include/execution/plans/seq_scan_plan.h
src/include/execution/plans/insert_plan.h
src/include/execution/plans/update_plan.h
src/include/execution/plans/delete_plan.h
src/include/execution/plans/nested_loop_join_plan.h
src/include/execution/plans/hash_join_plan.h
src/include/execution/plans/aggregation_plan.h
src/include/execution/plans/limit_plan.h
src/include/execution/plans/distinct_plan.h
```
These plan nodes already possess all of the data and functionality necessary to implement the required executors. You should not modify any of these plan nodes.

Executors accept tuples from their children (possibly multiple) and yield tuples to their parent. They may rely on conventions about the ordering of their children, which we describe below. You are also free to add private helper functions and class members as you see fit.

##### SEQUENTIAL SCAN

The SeqScanExecutor iterates over a table and return its tuples, one-at-a-time. A sequential scan is specified by a SeqScanPlanNode. The plan node specifies the table over which the scan is performed. The plan node may also contain a predicate; if a tuple does not satisfy the predicate, it is not produced by the scan.

> Hint: Be careful when using the TableIterator object. Make sure that you understand the difference between the pre-increment and post-increment operators. You may find yourself getting strange output by switching between ++iter and iter++.

> Hint: You will want to make use of the predicate in the sequential scan plan node. In particular, take a look at AbstractExpression::Evaluate. Note that this returns a Value, on which you can invoke GetAs<bool>() to evaluate the result as a native C++ boolean type.

> Hint: The output of sequential scan is a copy of each matched tuple and its original record identifier (RID).

##### INSERT

The InsertExecutor inserts tuples into a table and updates indexes. There are two types of inserts that your executor must support.

The first type are insert operations in which the values to be inserted are embedded directly inside the plan node itself. We refer to these as raw inserts.

The second type are inserts that take the values to be inserted from a child executor. For example, you may have an InsertPlanNode with a SeqScanPlanNode as is child to implement an INSERT INTO .. SELECT ...

You may assume that the InsertExecutor is always at the root of the query plan in which it appears. The InsertExecutor should not modify its result set.

> Hint: You will need to lookup table information for the target of the insert during executor initialization. See the System Catalog section below for additional information on accessing the catalog.

> Hint: You will need to update all indexes for the table into which tuples are inserted. See the Index Updates section below for further details.

> Hint: You will need to use the TableHeap class to perform table modifications.

##### UPDATE

The UpdateExecutor modifies existing tuples in a specified table and update its indexes. The child executor will provide the tuples (with their RID) that the update executor will modify.

Unlike the InsertExecutor, an UpdateExecutor always pulls tuples from a child executor to perform an update. For example, the UpdatePlanNode will have a SeqScanPlanNode as its child.

You may assume that the UpdateExecutor is always at the root of the query plan in which it appears. The UpdateExecutor should not modify its result set.

> Hint: We provide you with a GenerateUpdatedTuple that constructs an updated tuple for you based on the update attributes provided in the plan node.

> Hint: You will need to lookup table information for the target of the update during executor initialization. See the System Catalog section below for additional information on accessing the catalog.

> Hint: You will need to use the TableHeap class to perform table modifications.

> Hint: You will need to update all indexes for the table upon which the update is performed. See the Index Updates section below for further details.

##### DELETE

The DeleteExecutor deletes tuples from tables and removes their entries from all table indexes. Like updates, tuples to be deleted are pulled from a child executor, such as a SeqScanExecutor instance.

You may assume that the DeleteExecutor is always at the root of the query plan in which it appears. The DeleteExecutor should not modify its result set.

> Hint: You only need to get RID from the child executor and call TableHeap::MarkDelete() to effectively delete the tuple. All deletes will be applied upon transaction commit.
> Hint: You will need to update all indexes for the table from which tuples are deleted. See the Index Updates section below for further details.

##### NESTED LOOP JOIN

The NestedLoopJoinExecutor implements a basic nested loop join that combines the tuples from its two child executors together.

This executor should implement the simple nested loop join algorithm presented in lecture. That is, for each tuple in the join's outer table, you should consider each tuple in the join's inner table, and emit an output tuple if the join predicate is satisfied.

One of the learning objectives of this project is to give you a sense of the strengths and weaknesses of different physical implementations for logical operators. For this reason, it is important that your NestedLoopJoinExecutor actually implements the nested loop join algorithm, instead of, for example, reusing your implementation for the hash join executor (see below). To this end, we will test the IO cost of your NestedLoopJoinExecutor to determine if it implements the algorithm correctly.

Hint: You will want to make use of the predicate in the nested loop join plan node. In particular, take a look at AbstractExpression::EvaluateJoin, which handles the left tuple and right tuple and their respective schemas. Note that this returns a Value, on which you can invoke GetAs<bool>() to evaluate the result as a native C++ boolean type.

##### HASH JOIN

The HashJoinExecutor implements a hash join operation that combines the tuples from its two child executors together.

As the name suggests (and as was discussed in lecture), the hash join operation is implemented with the help of a hash table. For the purposes of this project, we make the simplifying assumption that the join hash table fits entirely in memory. This implies that you do not need to worry about spilling temporary partitions of the build-side table to disk in your implementation.

As in the NestedLoopJoinExecutor, we will test the IO cost of your HashJoinExecutor to determine if it implements the hash join algorithm correctly.

> Hint: Your implementation should correctly handle the case in which multiple tuples (on either side of the join) share a common join key.

> Hint: The HashJoinPlanNode defines the GetLeftJoinKey() and GetRightJoinKey() member functions; you should use the expressions returned by these accessors to construct the join keys for the left and right sides of the join, respectively.

> Hint: You will need a way to hash a tuple with multiple attributes in order to construct a unique key. As a starting point, take a look at how the SimpleAggregationHashTable in the AggregationExecutor implements this functionality.

> Hint: Recall that, in the context of a query plan, the build side of a hash join is a pipeline breaker. This may influence the way that you use the HashJoinExecutor::Init() and HashJoinExecutor::Next() functions in your implementation. In particular, think about whether the Build phase of the join should be performed in HashJoinExecutor::Init() or HashJoinExecutor::Next().

##### AGGREGATION

This executor combines multiple tuple results from a single child executor into a single tuple. In this project, we ask you to implement the COUNT, SUM, MIN, and MAX aggregations. Your AggregationExecutor must also support GROUP BY and HAVING clauses.

As discussed in lecture, a common strategy for implementing aggregation is to use a hash table. This is the method that you will use in this project, however, we make the simplifying assumption that the aggregation hash table fits entirely in memory. This implies that you do not need to worry about implementing the two-stage (Partition, Rehash) strategy for hash aggregations, and may instead assume that all of your aggregation results can reside in an in-memory hash table.

Furthermore, we provide you with the SimpleAggregationHashTable data structure that exposes an in-memory hash table (std::unordered_map) but with an interface designed for computing aggregations. This class also exposes the SimpleAggregationHashTable::Iterator type that can be used to iterate through the hash table.

> Hint: You will want to aggregate the results and make use of the HAVING clause for constraints. In particular, take a look at AbstractExpression::EvaluateAggregate, which handles aggregation evaluation for different types of expressions. Note that this returns a Value, on which you can invoke GetAs<bool>() to evaluate the result as a native C++ boolean type.

> Hint: Recall that, in the context of a query plan, aggregations are pipeline breakers. This may influence the way that you use the AggregationExecutor::Init() and AggregationExecutor::Next() functions in your implementation. In particular, think about whether the Build phase of the aggregation should be performed in AggregationExecutor::Init() or AggregationExecutor::Next().

##### LIMIT
The LimitExecutor constrains the number of output tuples from its child executor. If the number of tuples produced by its child executor is less than the limit specified in the LimitPlanNode, this executor has no effect and yields all of the tuples that it receives.

##### DISTINCT
The DistinctExecutor eliminates duplicate tuples received from its child executor. Your DistinctExecutor should consider all columns of the input tuple when determining uniqueness.

Like aggregations, distinct operators are typically implemented with the help of a hash table (this is a hash distinct operation). This is the approach that you will use in this project. As in the AggregationExecutor and HashJoinExecutor, you may assume that the hash table you use to implement your DistinctExecutor fits entirely in memory.

> Hint: You will need a way to hash a tuple with potentially-many attributes in order to construct a unique key. As a starting point, take a look at how the SimpleAggregationHashTable in the AggregationExecutor implements this functionality.

##### SYSTEM CATALOG
A database maintains an internal catalog to keep track of meta-data about the database. In this project, you will interact with the system catalog to query information regarding tables, indexes, and their schemas.

The entirety of the catalog implementation is in src/include/catalog.h. You should pay particular attention to the member functions Catalog::GetTable() and Catalog::GetIndex(). You will use these functions in the implementation of your executors to query the catalog for tables and indexes.

##### INDEX UPDATES
For the table modification executors (InsertExecutor, UpdateExecutor, and DeleteExecutor) you must modify all indexes for the table targeted by the operation. You may find the Catalog::GetTableIndexes() function useful for querying all of the indexes defined for a particular table. Once you have the IndexInfo instance for each of the table's indexes, you can invoke index modification operations on the underlying index structure.

In this project, we use your implementation of the extendible hash table from Project 2 as the underlying data structure for all index operations. Therefore, successful completion of this project relies on a working implementation of the extendible hash table.


#### TESTING
You can test the individual components of this assigment using our testing framework. We use GTest for unit test cases. We provide you with a single unit test file, test/execution/executor_test, that implements tests for basic functionality of some executors.
```bash
cd build
make executor_test
./test/executor_test
```
As you progress through the project, you can enable individual tests in executor_test by removing the DISABLED_ prefix from the test case names.