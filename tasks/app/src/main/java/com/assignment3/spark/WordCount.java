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
        String textFilePath = "input/pigs.txt";

        SparkConf conf = new SparkConf().setAppName("WordCountWithSpark").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sparkContext.textFile(textFilePath);

        JavaRDD<String> words = textFile.flatMap(new Filter());

        JavaPairRDD<String, Integer> counts = words.mapToPair(
                s -> new Tuple2<>(s, 1)
        );

        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
                Integer::sum
        );

        String outputPath = "output";
        reducedCounts.saveAsTextFile(outputPath);

        // Consolidate output into a single file.
        JavaRDD<String> consolidatedOutput = sparkContext.textFile(outputPath + "/*");
        String consolidatedOutputPath = outputPath + "/tempConsolidated";
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
