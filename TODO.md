## What Jian Have Done?

* Computation of touched node for range shard keys

	First partition every field into ranges in the workload analysis stage according to its distinct values. For example, if a field has distinct values [1,2,3,4,5,6,7,8], and we have 4 shards, the ranges arrays will be [1,3,5,7]. The number in the ranges array indicate the minimum value for that range. 
    
    Then we use the ranges information generated in the workload analysis stage to compute the touched node for range shard keys. If a query contains a equality by key A with value 6 in the above example, this query will access shard number 2(starts from 0).
    

* Candidate generation for shard keys

	We only choose shard keys with high cardinality and high referenced count. According to cardinality and referenced count, d4 generates a score for each key, then d4 sorts all keys by those scores. We set a threshold to filter out keys with low score. 
    
    When iterating on the combination of shard keys, the compound keys with more keys have higher priority to evaluate. 
    
    
* Estimation of number of shards

	Though the number of shards is set by user, however, not all collections could use all shards. For example, collections are sharded by keys with low cardinality or collections has small document size will be only sharded into subset of shards. So we need to estimate the number of shards for each collection with each design. Then use this number to calculate the cost.
    
    
* Latencies report for replay framework

	Add latencies report for replay framework, also output the top slowest queries for debugging usage.
    
* Lots of bug fixes

	Fixes bugs for input module, search algorithms, cost models and benchmark modules

## Future Work:

* [Issue 37](https://github.com/cmu-db/mongodb-d4/issues/37)
* [Issue 38](https://github.com/cmu-db/mongodb-d4/issues/38)
* [Issue 39](https://github.com/cmu-db/mongodb-d4/issues/37)
