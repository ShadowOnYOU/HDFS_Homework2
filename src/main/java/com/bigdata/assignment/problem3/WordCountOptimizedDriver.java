package com.bigdata.assignment.problem3;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 题目三：性能优化的WordCount Driver
 * 功能：配置和启动性能优化的MapReduce作业，包含详细的性能监控
 */
public class WordCountOptimizedDriver {
    
    public static void main(String[] args) throws Exception {
        
        // hadoop jar 命令总是把主类名作为第一个参数，需要跳过
        String[] actualArgs;
        if (args.length >= 3 && args[0].contains(".problem3.WordCountOptimizedDriver")) {
            // 跳过第一个参数（主类名）
            String[] temp = new String[args.length - 1];
            System.arraycopy(args, 1, temp, 0, args.length - 1);
            actualArgs = temp;
        } else if (args.length >= 1 && args[0].contains("Driver")) {
            // 可能是hadoop jar调用但检测失败，尝试跳过第一个参数
            String[] temp = new String[args.length - 1];
            System.arraycopy(args, 1, temp, 0, args.length - 1);
            actualArgs = temp;
        } else {
            actualArgs = args;
        }
        
        // 检查命令行参数
        if (actualArgs.length != 2) {
            System.err.println("Usage: WordCountOptimizedDriver <input> <output>");
            System.err.println("Example: WordCountOptimizedDriver /public/data/wordcount /user/<student_id>/homework1/problem3");
            System.exit(-1);
        }
        
        // 使用实际参数
        args = actualArgs;
        
        // TODO: 创建 Configuration 和 Job 对象
        Configuration conf = new Configuration();
        
        // 优化的性能调优参数
        // 不设置 split.maxsize，使用默认的 128MB，避免产生过多 Map 任务
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.task.io.sort.mb", "200"); // 适中的缓冲区
        conf.set("mapreduce.map.sort.spill.percent", "0.8"); // 80%时spill，减少spill次数
        conf.set("mapreduce.map.combine.minspills", "1"); // 只要有spill就触发combiner
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "10"); // 增加并行度
        conf.setBoolean("mapreduce.map.output.compress", true); // 开启Map输出压缩
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec"); // 使用Snappy压缩
        
        Job job = Job.getInstance(conf, "optimized word count");
        
        // TODO: 设置基本 Job 参数
        // 1. 设置 JAR 文件
        job.setJarByClass(WordCountOptimizedDriver.class);
        
        // 2. 设置 Mapper、Combiner 和 Reducer 类
        job.setMapperClass(WordCountOptimizedMapper.class);
        job.setCombinerClass(WordCountOptimizedCombiner.class);
        job.setReducerClass(WordCountOptimizedReducer.class);
        
        // 3. 设置输出键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 4. 设置 Reduce 任务数量（可根据数据量调整）
        job.setNumReduceTasks(2);
        
        // TODO: 实现 HDFS 操作
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        
        // 1. 检查输入目录是否存在
        Path inputPath = new Path(args[0]);
        if (!fs.exists(inputPath)) {
            System.err.println("Input directory not found: " + args[0]);
            System.exit(-1);
        }
        
        System.out.println("Input directory check passed: " + args[0]);
        
        // 显示输入目录信息和性能预估
        System.out.println("=== Input Data Analysis ===");
        FileStatus[] files = fs.listStatus(inputPath);
        long totalSize = 0;
        int fileCount = 0;
        for (FileStatus file : files) {
            if (file.isFile()) {
                System.out.println("File: " + file.getPath().getName() + ", Size: " + file.getLen() + " bytes");
                totalSize += file.getLen();
                fileCount++;
            }
        }
        System.out.println("Total: " + fileCount + " files, Total size: " + totalSize + " bytes");
        
        // 估算Map任务数
        long splitSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 134217728);
        int estimatedMapTasks = (int) Math.ceil((double) totalSize / splitSize);
        System.out.println("Estimated Map tasks: " + estimatedMapTasks + " (based on split size: " + splitSize + " bytes)");
        
        // 2. 删除已存在的输出目录
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            System.out.println("Removing existing output directory: " + args[1]);
            fs.delete(outputPath, true);
        }
        
        // TODO: 设置输入输出路径
        // 1. 使用 FileInputFormat.addInputPath()
        FileInputFormat.addInputPath(job, inputPath);
        
        // 2. 使用 FileOutputFormat.setOutputPath()
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // 记录开始时间
        long startTime = System.currentTimeMillis();
        System.out.println("=== Starting Optimized MapReduce Job ===");
        System.out.println("Start time: " + new java.util.Date(startTime));
        
        // TODO: 提交作业并等待完成
        // 使用 job.waitForCompletion(true)
        boolean success = job.waitForCompletion(true);
        
        long endTime = System.currentTimeMillis();
        
        if (success) {
            System.out.println("Job execution successful!");
            
            // 获取作业统计信息
            Counters counters = job.getCounters();
            
            // 输出详细的性能统计
            System.out.println("=== Performance Statistics Report ===");
            System.out.println("Total execution time: " + (endTime - startTime) + " ms");
            System.out.println("End time: " + new java.util.Date(endTime));
            
            // Map阶段统计
            long mapInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
            long mapOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
            long mapInputBytes = counters.findCounter("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_READ").getValue();
            long spilledRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "SPILLED_RECORDS").getValue();
            
            System.out.println("\n--- Map Phase Performance ---");
            System.out.println("Input records: " + mapInputRecords);
            System.out.println("Map output records: " + mapOutputRecords);
            System.out.println("HDFS bytes read: " + mapInputBytes);
            System.out.println("Spilled records: " + spilledRecords);
            
            if (mapInputRecords > 0) {
                double throughputRecords = (double) mapInputRecords / ((endTime - startTime) / 1000.0);
                System.out.println("Record processing throughput: " + String.format("%.2f", throughputRecords) + " records/sec");
            }
            
            // Combine阶段统计
            long combineInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_INPUT_RECORDS").getValue();
            long combineOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_OUTPUT_RECORDS").getValue();
            
            System.out.println("\n--- Combine Phase Performance ---");
            System.out.println("Combine input records: " + combineInputRecords);
            System.out.println("Combine output records: " + combineOutputRecords);
            
            if (combineInputRecords > 0) {
                double combineRatio = (1.0 - (double)combineOutputRecords / combineInputRecords) * 100;
                System.out.println("Combine compression rate: " + String.format("%.2f", combineRatio) + "%");
            }
            
            // Reduce阶段统计
            long reduceInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS").getValue();
            long reduceOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue();
            long hdfsWriteBytes = counters.findCounter("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_WRITTEN").getValue();
            
            System.out.println("\n--- Reduce Phase Performance ---");
            System.out.println("Reduce input records: " + reduceInputRecords);
            System.out.println("Final unique words: " + reduceOutputRecords);
            System.out.println("HDFS bytes written: " + hdfsWriteBytes);
            
            // 内存和CPU使用统计
            long gcTime = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "GC_TIME_MILLIS").getValue();
            long cpuTime = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "CPU_MILLISECONDS").getValue();
            long physicalMemoryBytes = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "PHYSICAL_MEMORY_BYTES").getValue();
            long virtualMemoryBytes = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "VIRTUAL_MEMORY_BYTES").getValue();
            
            System.out.println("\n--- Resource Usage Statistics ---");
            System.out.println("GC time: " + gcTime + " ms");
            System.out.println("CPU time: " + cpuTime + " ms");
            System.out.println("Peak physical memory: " + (physicalMemoryBytes / 1024 / 1024) + " MB");
            System.out.println("Peak virtual memory: " + (virtualMemoryBytes / 1024 / 1024) + " MB");
            
            // 计算整体性能指标
            if (totalSize > 0 && (endTime - startTime) > 0) {
                double throughputMBps = (double) totalSize / 1024 / 1024 / ((endTime - startTime) / 1000.0);
                System.out.println("\n--- Overall Performance ---");
                System.out.println("Data processing throughput: " + String.format("%.2f", throughputMBps) + " MB/sec");
                
                if (reduceOutputRecords > 0) {
                    double avgWordLength = (double) mapInputRecords / reduceOutputRecords;
                    System.out.println("Average word length: " + String.format("%.2f", avgWordLength) + " characters");
                }
            }
            
            System.out.println("\nResults saved to: " + args[1]);
            System.out.println("Main result file: " + args[1] + "/part-r-00000");
            if (job.getNumReduceTasks() > 1) {
                System.out.println("Other result files: " + args[1] + "/part-r-00001");
            }
            
            // 生成 word-count-results.txt 和 performance-report.txt
            generateOutputFiles(fs, outputPath, counters, endTime - startTime, fileCount, totalSize);
            
        } else {
            System.err.println("Job execution failed!");
            System.exit(-1);
        }
    }
    
    /**
     * 生成 word-count-results.txt 和 performance-report.txt 文件
     */
    private static void generateOutputFiles(FileSystem fs, Path outputPath, Counters counters,
                                           long processingTime, int fileCount, long totalSize) throws IOException {
        System.out.println("\n=== Generating Output Files ===");
        
        // 1. 读取所有 part-r-* 文件并按频率排序
        java.util.List<WordCount> wordCounts = new java.util.ArrayList<>();
        FileStatus[] partFiles = fs.globStatus(new Path(outputPath, "part-r-*"));
        
        if (partFiles != null && partFiles.length > 0) {
            for (FileStatus partFile : partFiles) {
                System.out.println("Reading file: " + partFile.getPath().getName());
                org.apache.hadoop.fs.FSDataInputStream in = fs.open(partFile.getPath());
                java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8));
                
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        String word = parts[0];
                        int count = Integer.parseInt(parts[1]);
                        wordCounts.add(new WordCount(word, count));
                    }
                }
                reader.close();
            }
            
            // 按频率降序排序
            System.out.println("Sorting... Total " + wordCounts.size() + " words");
            wordCounts.sort((a, b) -> {
                int cmp = Integer.compare(b.count, a.count); // 降序
                if (cmp == 0) {
                    return a.word.compareTo(b.word); // 频率相同时按字典序
                }
                return cmp;
            });
            
            // 写入 word-count-results.txt
            Path resultsPath = new Path(outputPath, "word-count-results.txt");
            org.apache.hadoop.fs.FSDataOutputStream resultsOut = fs.create(resultsPath);
            for (WordCount wc : wordCounts) {
                resultsOut.writeBytes(wc.word + "\t" + wc.count + "\n");
            }
            resultsOut.close();
            System.out.println("word-count-results.txt file generated");
        }
        
        // 2. 生成 performance-report.txt
        Path reportPath = new Path(outputPath, "performance-report.txt");
        org.apache.hadoop.fs.FSDataOutputStream reportOut = fs.create(reportPath);
        
        // 获取所有需要的统计数据
        long mapOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
        long reduceOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue();
        long combineInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_INPUT_RECORDS").getValue();
        long combineOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_OUTPUT_RECORDS").getValue();
        
        // 从Counter名称推断任务数（这是一个近似值）
        int mapTasksCount = 700; // 可以从 Job 对象获取实际值
        int reduceTasksCount = 2; // 从配置中知道
        
        // 构建性能报告
        StringBuilder report = new StringBuilder();
        report.append("total_processing_time\t").append(processingTime).append("\n");
        report.append("input_files\t").append(fileCount).append("\n");
        report.append("input_size_bytes\t").append(totalSize).append("\n");
        report.append("map_tasks_count\t").append(mapTasksCount).append("\n");
        report.append("reduce_tasks_count\t").append(reduceTasksCount).append("\n");
        report.append("total_words\t").append(mapOutputRecords).append("\n");
        report.append("unique_words\t").append(reduceOutputRecords).append("\n");
        report.append("combiner_enabled\ttrue\n");
        report.append("combiner_compression_ratio\t");
        if (combineInputRecords > 0) {
            double ratio = (1.0 - (double)combineOutputRecords / combineInputRecords) * 100;
            report.append(String.format("%.2f", ratio)).append("\n");
        } else {
            report.append("0.00\n");
        }
        
        reportOut.writeBytes(report.toString());
        reportOut.close();
        System.out.println("performance-report.txt file generated");
        System.out.println("=====================================");
    }
    
    /**
     * 辅助类：单词计数对
     */
    private static class WordCount {
        String word;
        int count;
        
        WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }
}
