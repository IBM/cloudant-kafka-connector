
# Performance Report

In the results, you can see that the number of `topics` and the type of `replication` has no significant impact of the performance. This insight is applicable for the source and sink connector. The parameters `batch.size`, `tasks` as well as the number of documents have a significant
 impact of the connector performance. Up to a certain `batch.size` the data throughput increase significantly. Also, the performance increase with a higher number of documents, because of the set-up time of the source and sink connector.

In summary, with the recommended parameter settings. The source and sink connector can achieve a data throughput between 5000 and 6000 documents per second. In combination, the end-to-end test results archieves a data rate between 4000 and 5000 documents per second.


## Configurations


The package `com.ibm.cloudant.kafka.performance` contains the performance tests of the connector. The package includes the test cases `CloudantSourcePerformanceTest`, `CloudantSinkPerformanceTest` and `CloudantSourceAndSinkPerformanceTest`   

Test 									|Description|Results
:--------------------------------------:|:--:|:--:
`CloudantSource PerformanceTest`		|Writes documents from a Cloudant database into Kafka topics|Time, Docs/Sec, Data/Sec
`CloudantSink PerformanceTest`			|Writes documents from Kafka topics into a Cloudant database|Time, Docs/Sec, Data/Sec
`CloudantSourceAndSink PerformanceTest` |Writes documents from a Cloudant database into Kafka topics and subsequently into another Cloudant database (End-to-End Test)|Time, Docs/Sec, Data/Sec  

The performance tests were executed 3 times each. The mean was taken from the 3 test runs respectively. All tests were executed with the default values listed in the table below. For your own individual performance tests, you may adapt the parameters in the test package `com.ibm.cloudant.kafka.performance` or the `test.properties`.

Parameter 			|Describtion 	|Default Value 			|Performance Impact
:-------------------|:--------------------------------------------------------------------------|:-------:|:--:
`benchmarkRounds`	|number of test rounds 														|3 	 	  |-
`warmupRounds`		|gives the JVM a chance to optimize the code 								|0   	  |-
`topics`			|A list of topics which are written in or read from kafka					|"topic1" |No
`batch.size`		|The batch size used to bulk read/write from a Cloudant database 			|10000	  |Yes
`tasks.max`			|number of concurrent threads for parallel operations				 		|1		  |Yes
`replication`		|The used schema to create ID's for the Cloundant objects					|false	  |No

## Results: Batch size
![Alt text](images/batch.png?raw=true "Batch Size")

`batch.size`|Source Test|Sink Test|End2End Test
-----------:|----------:|--------:|--:
1000		|3034		|1426	  |1204
2000		|4518		|2283	  |2040
3000		|4726		|2625	  |2221
4000		|5025		|3306	  |2499
5000		|5343		|3439	  |3124
10000		|5664		|4768	  |2379
15000		|5731		|5382	  |3571
20000		|5760		|5335	  |1587
25000		|5943		|5260	  |3124
30000		|5957		|4631	  |3125
40000		|5624		|4435	  |1816
50000		|5574		|3490	  |1098
60000		|5504		|2979	  |3123
70000		|5615		|3066	  |2703
80000		|5231		|2954	  |2688
90000		|5436		|2979	  |1428
100000		|5561		|2490	  |1123

Up to a certain `batch.size` the number of documents per second increase significantly. However, this changes after a certain `batch.size`. Then the `batch.size` has no significant effect or decrease on the number of documents per second. This might be due to a higher memory usage. Therefore the recommanded `batch.size` for the source and sink connector is 20000.    

## Results: Object schema (Replicate)
![Alt text](images/replication.png?raw=true "Replication")

`replication`|true|false
:----------:|--------:|--:
Sink Test	|5190	  |4768
End2End Test|4153	  |3703

The setting of the parameter `replication` has no significant impact on the data throughput of the sink connector. 

## Results: Topics
![Alt text](images/topics.png?raw=true "Topics")

`topics`|Source Test|Sink Test|End2End Test
:------:|----------:|--------:|--:
1		|4135		|4768	  |3703
2		|4036		|4882	  |3278
3		|4082		|4571	  |3529
4		|4128		|4365	  |3305
5		|4137		|4787	  |3289
6		|3442		|4475	  |3045

The number of `topics` have no significant impact on the data throughput of the source and sink connector.   

## Results: Documents
![Alt text](images/documents.png?raw=true "Documents")

Documents  |Source Test|Sink Test|End2End Test
----------:|----------:|--------:|--:
1		   |2		   |3	     |1
10	  	   |21		   |29     	 |2
100	   	   |178		   |209	     |25
1000	   |1161	   |1219	 |250
10000	   |2727	   |2330	 |1427
100000	   |3431	   |3694	 |1536
1000000	   |2689	   |3581	 |827

A higher number of documents increase the performance impact. This is because the set-up time of the source and sink connector are applied only once among a high number of documents. 

## Results: Tasks
![Alt text](images/tasks.png?raw=true "Tasks")

`tasks.max`|Source Test|Sink Test|End2End Test
:---------:|----------:|--------:|--:
1		   |4135	   |4768     |3703
2		   |000		   |000	     |000
3		   |000		   |000	     |000
4		   |000		   |000	     |000
5		   |000		   |000	     |000
6		   |000		   |000	     |000

[//]: <> (The performance of the connector is increasing with a higher number of tasks because of the parallel processing.)
No results