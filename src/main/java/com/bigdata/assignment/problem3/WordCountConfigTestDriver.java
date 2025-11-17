package com.bigdata.assignment.problem3;

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

import java.io.IOException;

/**
 * Enhanced WordCount Driver for Problem 3: Multiple Configuration Performance Testing
 * Tests different Map/Reduce task configurations for performance analysis
 */
public class WordCountConfigTestDriver {
    
    // Configuration test cases: {reduceTasks, splitSize, combinerEnabled, testName}
    // Simplified configurations to fit cluster resource limits (max memory: 3072MB)
    private static final Object[][] TEST_CONFIGS = {
        {1, 128L * 1024 * 1024, true, "config1_1reducer_128mb_combiner"},     // 1 reducer, 128MB splits, with combiner
        {2, 128L * 1024 * 1024, true, "config2_2reducer_128mb_combiner"},     // 2 reducers, 128MB splits, with combiner
        {2, 64L * 1024 * 1024, true, "config3_2reducer_64mb_combiner"},       // 2 reducers, 64MB splits, with combiner
        {2, 128L * 1024 * 1024, false, "config4_2reducer_128mb_nocombiner"},  // 2 reducers, 128MB splits, no combiner
    };

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WordCountConfigTestDriver <input path> <output base path>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputBasePath = args[1];

        System.out.println("=== MapReduce Performance Configuration Testing ===");
        System.out.println("Input Path: " + inputPath);
        System.out.println("Output Base Path: " + outputBasePath);
        System.out.println("Test Configurations: " + TEST_CONFIGS.length);
        System.out.println("================================================");

        // Run all test configurations
        for (int i = 0; i < TEST_CONFIGS.length; i++) {
            Object[] config = TEST_CONFIGS[i];
            int reduceTasks = (Integer) config[0];
            long splitSize = (Long) config[1];
            boolean combinerEnabled = (Boolean) config[2];
            String testName = (String) config[3];

            System.out.println("\n--- Running Test " + (i + 1) + "/" + TEST_CONFIGS.length + ": " + testName + " ---");
            
            try {
                runSingleTest(inputPath, outputBasePath + "/" + testName, 
                            reduceTasks, splitSize, combinerEnabled, testName);
                
                // Wait between tests to avoid resource conflicts
                Thread.sleep(5000);
                
            } catch (Exception e) {
                System.err.println("Error in test " + testName + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("\n=== All Performance Tests Completed ===");
        System.out.println("Check individual output directories for detailed results.");
    }

    private static void runSingleTest(String inputPath, String outputPath, 
                                    int reduceTasks, long splitSize, 
                                    boolean combinerEnabled, String testName) throws Exception {
        
        long startTime = System.currentTimeMillis();
        
        Configuration conf = new Configuration();
        
        // Set split size to control map tasks
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", splitSize);
        conf.setLong("mapreduce.input.fileinputformat.split.minsize", splitSize / 2);
        
        // Performance tuning settings
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.8");
        
        // Memory settings for performance (adjusted for cluster limits: max 3072MB)
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "1536");
        conf.set("mapreduce.map.java.opts", "-Xmx819m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx1228m");

        Job job = Job.getInstance(conf, "WordCount-ConfigTest-" + testName);
        job.setJarByClass(WordCountConfigTestDriver.class);
        
        // Set mapper and reducer
        job.setMapperClass(WordCountOptimizedMapper.class);
        job.setReducerClass(WordCountOptimizedReducer.class);
        
        // Set combiner if enabled
        if (combinerEnabled) {
            job.setCombinerClass(WordCountOptimizedCombiner.class);
        }
        
        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Set number of reduce tasks
        job.setNumReduceTasks(reduceTasks);

        // Delete output directory if exists
        FileSystem fs = FileSystem.get(conf);
        Path outputDir = new Path(outputPath);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, outputDir);

        System.out.println("Configuration Details:");
        System.out.println("  - Reduce Tasks: " + reduceTasks);
        System.out.println("  - Split Size: " + (splitSize / (1024 * 1024)) + " MB");
        System.out.println("  - Combiner Enabled: " + combinerEnabled);
        System.out.println("  - Output Path: " + outputPath);

        // Submit job and wait for completion
        boolean success = job.waitForCompletion(true);
        
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;

        if (success) {
            // Get job counters for detailed analysis
            Counters counters = job.getCounters();
            
            // Extract key performance metrics
            long inputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                   "MAP_INPUT_RECORDS").getValue();
            long mapOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                       "MAP_OUTPUT_RECORDS").getValue();
            long reduceInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                         "REDUCE_INPUT_RECORDS").getValue();
            long reduceOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                          "REDUCE_OUTPUT_RECORDS").getValue();
            
            long mapOutputBytes = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                     "MAP_OUTPUT_BYTES").getValue();
            long reduceShuffleBytes = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                         "REDUCE_SHUFFLE_BYTES").getValue();

            // Calculate combiner effectiveness
            double combinerReduction = 0.0;
            if (combinerEnabled && mapOutputRecords > 0) {
                combinerReduction = ((double)(mapOutputRecords - reduceInputRecords) / mapOutputRecords) * 100;
            }

            // Print performance summary
            System.out.println("\nPerformance Results for " + testName + ":");
            System.out.println("  Processing Time: " + processingTime + " ms (" + 
                             String.format("%.2f", processingTime / 1000.0) + " seconds)");
            System.out.println("  Input Records: " + inputRecords);
            System.out.println("  Map Output Records: " + mapOutputRecords);
            System.out.println("  Reduce Input Records: " + reduceInputRecords);
            System.out.println("  Reduce Output Records: " + reduceOutputRecords);
            System.out.println("  Map Output Bytes: " + String.format("%.2f MB", mapOutputBytes / (1024.0 * 1024.0)));
            System.out.println("  Shuffle Bytes: " + String.format("%.2f MB", reduceShuffleBytes / (1024.0 * 1024.0)));
            
            if (combinerEnabled) {
                System.out.println("  Combiner Reduction: " + String.format("%.2f%%", combinerReduction));
            }
            
            System.out.println("  Throughput: " + String.format("%.0f records/second", 
                             inputRecords * 1000.0 / processingTime));

            // Save detailed performance report
            savePerformanceReport(outputPath, testName, processingTime, counters, 
                                reduceTasks, splitSize, combinerEnabled, combinerReduction);

        } else {
            System.err.println("Job failed for configuration: " + testName);
        }
    }

    private static void savePerformanceReport(String outputPath, String testName, 
                                            long processingTime, Counters counters,
                                            int reduceTasks, long splitSize, 
                                            boolean combinerEnabled, double combinerReduction) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path reportPath = new Path(outputPath + "/performance-config-report.txt");
            
            StringBuilder report = new StringBuilder();
            report.append("=== Performance Configuration Test Report ===\n");
            report.append("Test Name: ").append(testName).append("\n");
            report.append("Timestamp: ").append(System.currentTimeMillis()).append("\n");
            report.append("\n=== Configuration Parameters ===\n");
            report.append("reduce_tasks\t").append(reduceTasks).append("\n");
            report.append("split_size_mb\t").append(splitSize / (1024 * 1024)).append("\n");
            report.append("combiner_enabled\t").append(combinerEnabled).append("\n");
            
            report.append("\n=== Performance Metrics ===\n");
            report.append("total_processing_time\t").append(processingTime).append("\n");
            
            // Extract and save all relevant counters
            long inputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                   "MAP_INPUT_RECORDS").getValue();
            long mapOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                       "MAP_OUTPUT_RECORDS").getValue();
            long reduceInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                         "REDUCE_INPUT_RECORDS").getValue();
            long reduceOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                          "REDUCE_OUTPUT_RECORDS").getValue();
            long mapOutputBytes = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                     "MAP_OUTPUT_BYTES").getValue();
            long shuffleBytes = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", 
                                                   "REDUCE_SHUFFLE_BYTES").getValue();

            report.append("input_records\t").append(inputRecords).append("\n");
            report.append("map_output_records\t").append(mapOutputRecords).append("\n");
            report.append("reduce_input_records\t").append(reduceInputRecords).append("\n");
            report.append("reduce_output_records\t").append(reduceOutputRecords).append("\n");
            report.append("map_output_bytes\t").append(mapOutputBytes).append("\n");
            report.append("shuffle_bytes\t").append(shuffleBytes).append("\n");
            report.append("combiner_reduction_rate\t").append(String.format("%.2f", combinerReduction)).append("\n");
            report.append("throughput_records_per_sec\t").append(String.format("%.0f", inputRecords * 1000.0 / processingTime)).append("\n");
            
            report.append("\n=== Analysis Notes ===\n");
            if (combinerEnabled) {
                report.append("Combiner reduced data transfer by ").append(String.format("%.1f%%", combinerReduction)).append("\n");
            } else {
                report.append("Combiner was disabled for this test\n");
            }
            
            if (reduceTasks == 1) {
                report.append("Single reducer configuration - no parallel reduce processing\n");
            } else {
                report.append("Multi-reducer configuration with ").append(reduceTasks).append(" parallel reducers\n");
            }

            // Write report to HDFS
            try (java.io.OutputStream out = fs.create(reportPath, true)) {
                out.write(report.toString().getBytes("UTF-8"));
            }
            
            System.out.println("  Performance report saved to: " + reportPath);
            
        } catch (IOException e) {
            System.err.println("Failed to save performance report: " + e.getMessage());
        }
    }
}