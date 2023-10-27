Assignment 3
------------

# Team Members
- Emma Kozmér
- Luisa Ella Mueller

# GitHub link to your repository (if submitting through GitHub)

https://github.com/luisaellamaria/Assignment3.git

# Task 2

We had some thoughts about the difference between the local implementation of the word count algorithm and the cluster mode implementation. Since we performed it only on a small dataset, we did not notice any runtime difference. However, obviously the runtime of the localy implementd word count algorithm totaly depends on the machine it is running on, whereas the cluster mode implementation leverages the power of multiple nodes, so that the data can be processed parallel accross different machines. Further, the cluster mode implementation is hihgly scalable. As the dataset grows there can just be added more nodes to the cluster, so that there is an optimal utilization of resources. Therefore, it is likely that the cluster mode implementation performs better on larger datasets with regard to the runtime of the program. 

# Task 3

1. How does Spark optimize its file access compared to the file access in MapReduce?
> Ans: In comparison to MapReduce, Spark offers a general-purpose distributed computing framework, which is designed to process data faster. To do that, Spark processes and retains data in memory (in-memory processing storing data in the RAM) for subsequent steps. MapReduce on the other hand, processes data on disk, which makes it way slower.
> We got the imformation from the lecture (week 6)

2. In your implementation of WordCount (task1), did you use ReduceByKey or groupByKey method? 
   What does your preferred method do in your implementation? 
   What are the differences between the two methods in Spark?
> Ans: In our implementation of WordCount (task1) we used the reduceByKey() method. This method aggregates the counts of each unique word (key-value pairs with word as key and value 1) using a reduce function. The process results in key-value pairs with a word as the key and the total count of occurences of each word as the value. The groupByKey() method, on the other hand groups, the words by key and returns a PairRDD where each key is associated with a list of values, but compared to the reduceByKey() method no aggregation is performed.
> We got the information about the groupByKey() method from here: https://www.linkedin.com/pulse/apache-spark-difference-between-reducebykey-abhijit-sarkhel/

3. Explain what Resilient Distributed Dataset (RDD) is and the benefits it brings to the classic MapReduce model.
> Ans: A Resilient Distributed Dataset (RDD) is a fundamental data abstraction of Apache Spark. It is an immutable (static) distributed collection of objects, resulted out of parallel transformations such as map, filter, etc. So, the transformation of an existing RDD will result in a new RDD. RDDs can be distributed across many nodes in a cluster, what enables parallel processing of data. The objects are resilient, because they can be rebuilt if lost, while using lineage information. In comparison, MapReduce achieves fault tolerance through data replication in HDFS. MapReduce also stores intermediate data on disk, leading to higher latencies.
> We got the information about the RDD from the lecture (week5)


4. Imagine that you have a large dataset that needs to be processed in parallel. How would you partition the dataset efficiently and keep control over the number of outputs created at the end of the execution?
> Ans: Since we learned something about Spark in this assignment (and also the lectures), we know avout the partition tuning in Spark. In this case, the key-point would be to find a resonalbe number of partitions that fit the best with the given dataset. Having too few paritions would be suboptimal, because concurrency would be too low and large partitions increase memory preassure. Having too many partitions is also suboptimal, because it would lead to too much scheduling overhead. We learned from the lecture that, there exists a loverbound which is at least 2X number of CPU cores in the cluster) and an upper bound which is to ensure that tasks take at least 100ms. So, we sould partition or dataset according to this bounds. While setting the number of paritions in Apache Spark f.e., we would be directly able to control the number of outputs created. 

  If a task is stuck on the Spark cluster due to a network issue that the cluster had during execution, which methods can be used to retry or restart the task execution on a node?
> Ans: Firstly, Spark is designed to be fault-tolerant and resilient. So, if a task fails, Spark will automatically retry the task on a different node of the cluster. Further, the number of retries can be configured using the spark.task.maxFailures configuration parameter. Another possibility would be to manually remove and restart a problematic node from the cluster. There should be also a feature called speculative execution wherein it can start a duplicate copy of a slow-running task.
> We did not know about the speculative execution of spark, hence we got the information from here: https://kb.databricks.com/scala/understanding-speculative-execution#:~:text=Speculative%20execution%20can%20be%20used%20to%20unblock%20a%20Spark%20application,issue%20and%20disable%20speculative%20execution.
