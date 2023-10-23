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
            s = s.replaceAll("[^a-zA-Z\\s]", ""); // Remove non-alphabetic characters
            String[] subStrings = s.toLowerCase().split("\\s+");
            return Arrays.asList(subStrings).iterator();
        }
    }

    public static void main(String[] args) {
        String textFilePath = "input/pigs.txt"; // input file path

        SparkConf conf = new SparkConf().setAppName("WordCountWithSpark").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sparkContext.textFile(textFilePath);
        JavaRDD<String> words = textFile.flatMap(new Filter());

        // Map each word to a tuple of (word, 1)
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                s -> new Tuple2<>(s, 1)
        );

        // Reduce the tuples by key (word) and sum the values (counts)
        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
                Integer::sum
        );

        String outputPath = "output"; // output directory path

        // Check and delete the output directory if it exists
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

        // Save the output to the output directory
        reducedCounts.saveAsTextFile(outputPath);

        // Consolidate output into a single file.
        JavaRDD<String> consolidatedOutput = sparkContext.textFile(outputPath + "/*");
        String consolidatedOutputPath = outputPath + "/tempConsolidated"; // temporary consolidated output directory path
        consolidatedOutput.coalesce(1).saveAsTextFile(consolidatedOutputPath);

        // Rename and move the consolidated file back to the original output directory.
        try {
            FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration());
            Path oldPath = new Path(consolidatedOutputPath + "/part-00000");
            Path newPath = new Path(outputPath + "/output-task1.txt");
            fs.rename(oldPath, newPath);
            fs.delete(new Path(outputPath + "/_SUCCESS"), true);
            fs.delete(new Path(outputPath + "/tempConsolidated"), true);
        } catch (Exception e) {
            System.err.println("Error renaming the file: " + e);
        }

        sparkContext.stop();
        sparkContext.close();
    }
}
