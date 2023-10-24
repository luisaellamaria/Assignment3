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
> Ans: In comparison to MapReduce, Spark offers several optimizations. It has an in-memory processing, while it stores data in the RAM rather than writing it to the disk. This leads to much faster data processing like MapReduce. It also uses the DAG to shuffle data, which can lead to large performance gains. In MapReduce the suffling happens between the Map and Reduce stages.

2. In your implementation of WordCount (task1), did you use ReduceByKey or groupByKey method? 
   What does your preferred method do in your implementation? 
   What are the differences between the two methods in Spark?
> Ans: The ReduceByKey method aggregates the counts of each unique word. For each word, a key-value pair is created where the word is the key and the value is 1. The ReducedByKey method sums up these values for each unique word, effectively giving the total cout of occurences for each word in the dataset. 

3. Explain what Resilient Distributed Dataset (RDD) is and the benefits it brings to the classic MapReduce model.
> Ans: An RDD, which stands for Resilient Distributed Dataset, is a fundamental data structure of Apache Spark. It is an immutable distributed collection of objects that can be processed in parallel. Here are some key points about RDDs:
Immutability: Once an RDD is created, it cannot be changed. However, you can apply transformations to an RDD to create a new one.
Partitioned: Data in RDD is split into multiple partitions, which can be processed on different nodes of a cluster.
Fault Tolerance: RDDs inherently provide fault tolerance. If a node fails, the RDD can be rebuilt using lineage information. The lineage info contains the series of transformations used to build the RDD.

4. Imagine that you have a large dataset that needs to be processed in parallel. 
   How would you partition the dataset efficiently and keep control over the number of outputs created at the end of the execution?
> Ans: 

  If a task is stuck on the Spark cluster due to a network issue that the cluster had during execution, 
  which methods can be used to retry or restart the task execution on a node?
> Ans: 
