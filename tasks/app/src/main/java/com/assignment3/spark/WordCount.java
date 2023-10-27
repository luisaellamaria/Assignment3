package com.assignment3.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;


public class WordCount {
    static class Filter implements FlatMapFunction<String, String>
    {
        @Override
        public Iterator<String> call(String s) {
            s = s.replaceAll("[^a-zA-Z\\s]", ""); // remove non-alphabetic characters from the line of the input file
            String[] subStrings = s.toLowerCase().split("\\s+"); // convert string to lowercase and split into words
            return Arrays.asList(subStrings).iterator(); // return the needed iterator (it will be used by Spark to process each word separately)
        }
    }

    public static void main(String[] args) {
        //String textFilePath = "input/pigs.txt"; // input file path for task1
        String textFilePath = "hdfs://172.20.10.3:9000/sparkApp/input/pigs.txt";// input file path for task2

        // conf for task1
        SparkConf conf = new SparkConf().setAppName("WordCountWithSpark").setMaster("local[*]"); // create a SparkConf object

        JavaSparkContext sparkContext = new JavaSparkContext(conf); // create a JavaSparkContext object (allows to interact with Spark)

        JavaRDD<String> textFile = sparkContext.textFile(textFilePath); // read input to a JavaRDD (resilient distributed dataset) object
        JavaRDD<String> words = textFile.flatMap(new Filter()); // split each line into words

        // map each word to a tuple of (word, 1)
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                s -> new Tuple2<>(s, 1)
        );

        // reduce the tuples by key (word) and sum the values (counts)
        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
                Integer::sum
        );

        //String outputPath = "output"; // output directory path task 1
        String outputPath = "hdfs://172.20.10.3:9000/sparkApp/output"; // output directory path task 2

        // check and delete the output directory if it exists
        try {
            FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration());
            Path outputPathDir = new Path(outputPath);
            if (fs.exists(outputPathDir)) {
                fs.delete(outputPathDir, true);
                System.out.println("Existing output directory found and deleted.");
            }
        } catch (Exception e) {
            System.err.println("Error checking/deleting the output directory: " + e);
        }

        reducedCounts.saveAsTextFile(outputPath); // save the output to the output directory

        // consolidation of the output into a single file
        JavaRDD<String> consolidatedOutput = sparkContext.textFile(outputPath + "/*");
        String consolidatedOutputPath = outputPath + "/tempConsolidated"; // temporary consolidated output directory path
        consolidatedOutput.coalesce(1).saveAsTextFile(consolidatedOutputPath); // consolidate new RDD object (consolidatedOutput) into a single file

        // rename and move the consolidated file back to the original output directory
        try {
            FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration());
            Path oldPath = new Path(consolidatedOutputPath + "/part-00000"); // consolidated file
            Path newPath = new Path(outputPath + "/output-task1.txt"); // new file name
            fs.rename(oldPath, newPath); // rename the file
            fs.delete(new Path(outputPath + "/_SUCCESS"), true); // delete the _SUCCESS file
            fs.delete(new Path(outputPath + "/tempConsolidated"), true); // delete the temporary consolidated output directory
        } catch (Exception e) {
            System.err.println("Error renaming the file: " + e);
        }

        // close the Spark context to release the resources
        sparkContext.stop();
        sparkContext.close();
    }
}
