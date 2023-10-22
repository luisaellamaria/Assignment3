package com.assignment3.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; // Correct import for Path
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
            s = s.replaceAll("[^a-zA-Z\\s]", ""); // Remove non-alphabetic characters
            String[] subStrings = s.toLowerCase().split("\\s+");
            return Arrays.asList(subStrings).iterator();
        }


    }

    public static void main(String[] args) {
        // Define the input file path
        String textFilePath = "input/pigs.txt";

        // Set up Spark configuration and context
        SparkConf conf = new SparkConf().setAppName("WordCountWithSpark").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load the input text file into an RDD
        JavaRDD<String> textFile = sparkContext.textFile(textFilePath);

        // Use the Filter class to split the text into words
        JavaRDD<String> words = textFile.flatMap(new Filter());

        // Map each word to a tuple (word, 1)
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );

        // Reduce the tuples by key to sum up the frequencies
        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer a, Integer b) {
                        return a + b;
                    }
                }
        );
        // Save the result to an output directory. If output directory exists, delete it first.
        String outputPath = "output";
        reducedCounts.saveAsTextFile(outputPath);

        // Consolidate output into a single file. This ensures only one part file is generated.
        JavaRDD<String> consolidatedOutput = sparkContext.textFile(outputPath + "/*"); // read all parts
        String consolidatedOutputPath = "consolidatedOutput";
        consolidatedOutput.coalesce(1).saveAsTextFile(consolidatedOutputPath);

        // Rename the file
        try {
            FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration());
            Path oldPath = new Path(consolidatedOutputPath + "/part-00000");
            Path newPath = new Path(consolidatedOutputPath + "/output-task1.txt");
            fs.rename(oldPath, newPath);
        } catch (Exception e) {
            System.err.println("Error renaming the file: " + e); // replaced logger with a print statement
        }
        // Close the Spark context
        sparkContext.stop();
        sparkContext.close();
    }
}
