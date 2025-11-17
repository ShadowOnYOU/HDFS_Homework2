package com.bigdata.assignment.problem2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.IOException;
import java.util.Date;

/**
 * Enhanced WordCount Driver with Performance Comparison
 * Compares performance with and without Combiner
 * 
 * Usage:
 * hadoop jar jar_file WordCountWithPerformanceDriver input_path output_path [true|false]
 * 
 * The third parameter controls whether to enable Combiner:
 * - true: Enable Combiner (default)
 * - false: Disable Combiner for performance comparison
 */
public class WordCountWithPerformanceDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: WordCountWithPerformanceDriver <input path> <output path> [enable_combiner true|false]");
            System.err.println("Examples:");
            System.err.println("  hadoop jar jar_file WordCountWithPerformanceDriver /input /output");
            System.err.println("  hadoop jar jar_file WordCountWithPerformanceDriver /input /output true");
            System.err.println("  hadoop jar jar_file WordCountWithPerformanceDriver /input /output false");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        boolean enableCombiner = args.length == 3 ? Boolean.parseBoolean(args[2]) : true;

        System.out.println("=== WordCount with Performance Analysis ===");
        System.out.println("Input path: " + inputPath);
        System.out.println("Output path: " + outputPath);
        System.out.println("Combiner enabled: " + enableCombiner);
        System.out.println("Start time: " + new Date());
        System.out.println("==========================================");

        // Record start time for performance measurement
        long startTime = System.currentTimeMillis();

        // Create configuration and job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount-performance-analysis");
        
        // Set job configuration
        job.setJarByClass(WordCountWithPerformanceDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // Conditionally set combiner based on parameter
        if (enableCombiner) {
            job.setCombinerClass(WordCountCombiner.class);
            System.out.println("Combiner enabled: WordCountCombiner");
        } else {
            System.out.println("Combiner disabled for performance comparison");
        }

        // Set partitioner and number of reduce tasks
        job.setPartitionerClass(AlphabetPartitioner.class);
        job.setNumReduceTasks(4);

        // Set output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Check and handle HDFS paths
        FileSystem fs = FileSystem.get(conf);
        Path inputDir = new Path(inputPath);
        Path outputDir = new Path(outputPath);

        // Check if input directory exists
        if (!fs.exists(inputDir)) {
            System.err.println("Error: Input directory does not exist: " + inputPath);
            System.exit(-1);
        }

        // Delete output directory if it exists
        if (fs.exists(outputDir)) {
            System.out.println("Deleting existing output directory: " + outputPath);
            fs.delete(outputDir, true);
        }

        // Set input and output paths
        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Submit job and wait for completion
        boolean success = job.waitForCompletion(true);
        
        // Record end time
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        System.out.println("\n=== Performance Analysis Results ===");
        System.out.println("Job completion status: " + (success ? "SUCCESS" : "FAILED"));
        System.out.println("Total processing time: " + totalTime + " ms (" + (totalTime / 1000.0) + " seconds)");
        System.out.println("Combiner enabled: " + enableCombiner);
        System.out.println("End time: " + new Date());

        if (success) {
            // Get job counters for detailed analysis
            Counters counters = job.getCounters();
            
            // Map-Reduce framework counters
            Counter mapInputRecords = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS);
            Counter mapOutputRecords = counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
            Counter mapOutputBytes = counters.findCounter(TaskCounter.MAP_OUTPUT_BYTES);
            Counter reduceInputRecords = counters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS);
            Counter reduceOutputRecords = counters.findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
            Counter combineInputRecords = counters.findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
            Counter combineOutputRecords = counters.findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
            Counter shuffledMaps = counters.findCounter(TaskCounter.SHUFFLED_MAPS);
            Counter spilledRecords = counters.findCounter(TaskCounter.SPILLED_RECORDS);

            System.out.println("\n=== Detailed Performance Metrics ===");
            System.out.println("Map Input Records: " + mapInputRecords.getValue());
            System.out.println("Map Output Records: " + mapOutputRecords.getValue());
            System.out.println("Map Output Bytes: " + mapOutputBytes.getValue() + " (" + 
                             String.format("%.2f MB", mapOutputBytes.getValue() / (1024.0 * 1024.0)) + ")");
            
            if (enableCombiner && combineInputRecords.getValue() > 0) {
                System.out.println("\n--- Combiner Performance ---");
                System.out.println("Combiner Input Records: " + combineInputRecords.getValue());
                System.out.println("Combiner Output Records: " + combineOutputRecords.getValue());
                long reductionRecords = combineInputRecords.getValue() - combineOutputRecords.getValue();
                double reductionRate = (double) reductionRecords / combineInputRecords.getValue() * 100;
                System.out.println("Data Reduction by Combiner: " + reductionRecords + 
                                 " records (" + String.format("%.2f%%", reductionRate) + ")");
                System.out.println("Combiner Efficiency: " + String.format("%.2f:1", 
                                 (double) combineInputRecords.getValue() / combineOutputRecords.getValue()));
            } else {
                System.out.println("\n--- Combiner Performance ---");
                System.out.println("Combiner: DISABLED or NO DATA PROCESSED");
            }

            System.out.println("\n--- Shuffle Performance ---");
            System.out.println("Reduce Input Records: " + reduceInputRecords.getValue());
            System.out.println("Reduce Output Records: " + reduceOutputRecords.getValue());
            System.out.println("Shuffled Maps: " + shuffledMaps.getValue());
            System.out.println("Spilled Records: " + spilledRecords.getValue());

            // Calculate additional metrics
            if (mapOutputRecords.getValue() > 0) {
                double bytesPerRecord = (double) mapOutputBytes.getValue() / mapOutputRecords.getValue();
                System.out.println("Average Bytes per Map Output Record: " + String.format("%.2f", bytesPerRecord));
            }

            if (totalTime > 0) {
                double recordsPerSecond = (double) mapInputRecords.getValue() / (totalTime / 1000.0);
                double mbPerSecond = (mapOutputBytes.getValue() / (1024.0 * 1024.0)) / (totalTime / 1000.0);
                System.out.println("\n--- Throughput Analysis ---");
                System.out.println("Processing Rate: " + String.format("%.2f", recordsPerSecond) + " records/second");
                System.out.println("Data Processing Rate: " + String.format("%.2f", mbPerSecond) + " MB/second");
            }

            // Performance recommendations
            System.out.println("\n=== Performance Recommendations ===");
            if (enableCombiner) {
                if (combineInputRecords.getValue() > 0) {
                    double reductionRate = (double) (combineInputRecords.getValue() - combineOutputRecords.getValue()) 
                                         / combineInputRecords.getValue() * 100;
                    if (reductionRate > 50) {
                        System.out.println("✓ Combiner is HIGHLY EFFECTIVE (reduction rate: " + 
                                         String.format("%.2f%%", reductionRate) + ")");
                        System.out.println("  Combiner significantly reduces shuffle data volume");
                    } else if (reductionRate > 20) {
                        System.out.println("✓ Combiner is MODERATELY EFFECTIVE (reduction rate: " + 
                                         String.format("%.2f%%", reductionRate) + ")");
                    } else {
                        System.out.println("⚠ Combiner has LIMITED EFFECTIVENESS (reduction rate: " + 
                                         String.format("%.2f%%", reductionRate) + ")");
                        System.out.println("  Consider reviewing combiner logic or data characteristics");
                    }
                } else {
                    System.out.println("⚠ Combiner enabled but no data processed");
                }
            } else {
                System.out.println("ℹ Combiner disabled - for comparison with enabled version");
                System.out.println("  Run with combiner enabled to see performance improvement");
            }

            // Partition balance analysis
            System.out.println("\n--- Partition Balance Analysis ---");
            System.out.println("Number of Reduce Tasks: 4");
            System.out.println("Expected: Relatively even distribution across partitions");
            System.out.println("Verify: Check output files (part-r-00000 to part-r-00003) for size balance");

            System.out.println("\n=== Next Steps for Performance Comparison ===");
            if (enableCombiner) {
                System.out.println("1. Run the same job with combiner disabled:");
                System.out.println("   hadoop jar jar_file WordCountWithPerformanceDriver " + inputPath + " " + outputPath + "_no_combiner false");
                System.out.println("2. Compare the metrics, especially:");
                System.out.println("   - Total processing time");
                System.out.println("   - Reduce input records (shuffle data volume)");
                System.out.println("   - Network I/O and spilled records");
            } else {
                System.out.println("1. Run the same job with combiner enabled:");
                System.out.println("   hadoop jar jar_file WordCountWithPerformanceDriver " + inputPath + " " + outputPath + "_with_combiner true");
                System.out.println("2. Compare the results to see combiner optimization benefits");
            }

            System.out.println("\n=====================================");
            System.out.println("Performance analysis completed successfully!");
            
        } else {
            System.err.println("Job failed! Check the logs for details.");
            System.exit(-1);
        }

        System.exit(success ? 0 : 1);
    }
}