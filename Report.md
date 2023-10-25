Assignment 3
------------

# Team Members
- Emma Kozmér
- Luisa Ella Mueller

# GitHub link to your repository (if submitting through GitHub)

https://github.com/luisaellamaria/Assignment3.git

# Task 2

Performance difference is ...

# Task 3

1. How does Spark optimize its file access compared to the file access in MapReduce?
> Ans: In comparison to MapReduce, Spark offers a general-purpose distributed computing framework, which is designed to process data faster. To do that, Spark processes and retains data in memory (in-memory processing storing data in the RAM) for subsequent steps. MapReduce on the other hand, processes data on disk, which makes it way slower.
> We found a very helpful article about the differences between Apache Spark and MapReduce from which we also got the information above: https://www.knowledgehut.com/blog/big-data/apache-spark-and-mapreduce-comparison

2. In your implementation of WordCount (task1), did you use ReduceByKey or groupByKey method? 
   What does your preferred method do in your implementation? 
   What are the differences between the two methods in Spark?
> Ans: In our implementation of WordCount (task1) we used the reduceByKey() method. This method aggregates the counts of each unique word (key-value pairs with word as key and value 1) using a reduce function. The process results in key-value pairs with a word as the key and the total count of occurences of each word as the value. The groupByKey() method on the other hand groups the words by key and returns a PairRDD where each key is associated with a list of values, but compared to the reduceByKey() method no aggregation is performed.
> We got the information about the groupByKey() method from here: https://www.linkedin.com/pulse/apache-spark-difference-between-reducebykey-abhijit-sarkhel/

3. Explain what Resilient Distributed Dataset (RDD) is and the benefits it brings to the classic MapReduce model.
> Ans: An Resilient Distributed Dataset (RDD) is a fundamental data abstraction of Apache Spark. It is an immutable (static) distributed collection of objects that can be processed in parallel. RDDs are used to reduce the usage of external storage systems by leveraging in-memory compute operation storage.
>  Here are some key points about RDDs:
Immutability: Once an RDD is created, it cannot be changed. However, you can apply transformations to an RDD to create a new one.
Partitioned: Data in RDD is split into multiple partitions, which can be processed on different nodes of a cluster.
Fault Tolerance: RDDs inherently provide fault tolerance. If a node fails, the RDD can be rebuilt using lineage information. The lineage info contains the series of transformations used to build the RDD.

4. Imagine that you have a large dataset that needs to be processed in parallel. 
   How would you partition the dataset efficiently and keep control over the number of outputs created at the end of the execution?
> Ans: 

  If a task is stuck on the Spark cluster due to a network issue that the cluster had during execution, 
  which methods can be used to retry or restart the task execution on a node?
> Ans: 
