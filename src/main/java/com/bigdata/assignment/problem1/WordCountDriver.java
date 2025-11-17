package com.bigdata.assignment.problem1;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 题目一：WordCount Driver主程序
 * 功能：配置和启动MapReduce作业，处理HDFS文件操作
 */
public class WordCountDriver {
    
    // 公共数据目录路径
    private static final String INPUT_PATH = "/public/data/wordcount/";
    
    public static void main(String[] args) throws Exception {
        
        // hadoop jar 命令总是把主类名作为第一个参数，需要跳过
        String[] actualArgs;
        if (args.length >= 3 && args[0].contains(".problem1.WordCountDriver")) {
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
            System.err.println("Usage: WordCountDriver <input> <output>");
            System.err.println("Example: WordCountDriver /public/data/wordcount /user/<student_id>/homework1/problem1");
            System.exit(-1);
        }
        
        // 使用实际参数
        args = actualArgs;
        
        // TODO: 创建 Configuration 和 Job 对象
        Configuration conf = new Configuration();
        
        // 可选：设置一些Hadoop配置参数
        conf.set("mapreduce.job.jar", "hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar");
        
        Job job = Job.getInstance(conf, "word count problem 1");
        
        // TODO: 设置 Job 参数
        // 1. 设置 JAR 文件
        job.setJarByClass(WordCountDriver.class);
        
        // 2. 设置 Mapper 和 Reducer 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        
        // 3. 设置输出键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // TODO: 实现 HDFS 操作
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        
        // 1. 检查输入目录是否存在
        Path inputPath = new Path(args[0]);
        if (!fs.exists(inputPath)) {
            System.err.println("Error: Input directory not found: " + args[0]);
            System.exit(-1);
        }
        
        System.out.println("Input directory check passed: " + args[0]);
        
        // 显示输入目录信息
        displayInputInfo(fs, inputPath);
        
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
        
        // TODO: 提交作业并等待完成
        // 使用 job.waitForCompletion(true)
        boolean success = job.waitForCompletion(true);
        
        // 记录结束时间
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        
        if (success) {
            System.out.println("Job execution successful!");
            
            // TODO: 显示处理结果和统计信息，并保存到个人目录
            displayJobStatistics(job, processingTime, fs, outputPath, inputPath);
            
            // 生成 words.txt 文件（合并 part-r-* 文件）
            mergeOutputFiles(fs, outputPath);
            
        } else {
            System.err.println("Job execution failed!");
            System.exit(-1);
        }
        
        fs.close();
        System.exit(success ? 0 : 1);
    }
    
    /**
     * 显示输入目录信息
     */
    private static void displayInputInfo(FileSystem fs, Path inputPath) throws IOException {
        System.out.println("=== Input Directory Information ===");
        FileStatus[] fileStatuses = fs.listStatus(inputPath);
        int fileCount = 0;
        long totalSize = 0;
        
        for (FileStatus status : fileStatuses) {
            if (status.isFile()) {
                fileCount++;
                totalSize += status.getLen();
                System.out.println(String.format("File: %s, Size: %d bytes", 
                    status.getPath().getName(), status.getLen()));
            }
        }
        
        System.out.println(String.format("Total: %d files, Total size: %d bytes", fileCount, totalSize));
        System.out.println();
    }
    
    /**
     * 显示作业统计信息并保存到文件
     */
    private static void displayJobStatistics(Job job, long processingTime, 
                                           FileSystem fs, Path outputPath, Path inputPath) throws Exception {
        
        System.out.println("=== Job Execution Statistics ===");
        
        // 1. 从 Job 中获取 Counters 信息
        Counters counters = job.getCounters();
        
        // 获取输入记录数
        long inputRecords = counters.findCounter(
            "org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
        
        // 获取输出记录数
        long outputRecords = counters.findCounter(
            "org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue();
        
        // 获取Map输出记录数（总单词数）
        long totalWords = counters.findCounter(
            "org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
        
        // 获取输入文件数量
        int inputFileCount = getInputFileCount(fs, inputPath);
        
        // 2. 打印统计信息
        System.out.println("Processing time: " + processingTime + " ms");
        System.out.println("Input files: " + inputFileCount);
        System.out.println("Input records: " + inputRecords);
        System.out.println("Total words: " + totalWords);
        System.out.println("Unique words: " + outputRecords);
        
        // 3. 按照要求保存 statistics.txt 文件
        saveStatistics(fs, outputPath, processingTime, inputRecords, totalWords, outputRecords, inputFileCount);
        
        System.out.println("\nResults saved to: " + outputPath);
        System.out.println("Main result file: " + outputPath + "/part-r-00000");
        System.out.println("Statistics file: " + outputPath + "/statistics.txt");
    }
    
    /**
     * 获取输入文件数量
     */
    private static int getInputFileCount(FileSystem fs, Path inputPath) {
        try {
            if (fs.isFile(inputPath)) {
                // 如果是单个文件
                return 1;
            } else {
                // 如果是目录，统计其中的文件数量
                FileStatus[] fileStatuses = fs.listStatus(inputPath);
                int fileCount = 0;
                for (FileStatus status : fileStatuses) {
                    if (status.isFile()) {
                        fileCount++;
                    }
                }
                return fileCount;
            }
        } catch (Exception e) {
            return 0;
        }
    }
    
    /**
     * 保存统计信息到文件
     */
    private static void saveStatistics(FileSystem fs, Path outputPath, 
                                     long processingTime, long inputRecords, 
                                     long totalWords, long outputRecords, int inputFileCount) throws IOException {
        
        Path statisticsPath = new Path(outputPath, "statistics.txt");
        
        StringBuilder statistics = new StringBuilder();
        statistics.append("input_files\t").append(inputFileCount).append("\n");
        statistics.append("processing_time\t").append(processingTime).append("\n");
        statistics.append("total_words\t").append(totalWords).append("\n");
        statistics.append("unique_words\t").append(outputRecords).append("\n");
        
        // 写入统计信息文件
        org.apache.hadoop.fs.FSDataOutputStream out = fs.create(statisticsPath);
        out.writeBytes(statistics.toString());
        out.close();
        
        System.out.println("Statistics saved to: " + statisticsPath);
    }
    
    /**
     * 合并所有 part-r-* 文件为 words.txt
     * 输出格式：word\tcount（按字典序排序）
     */
    private static void mergeOutputFiles(FileSystem fs, Path outputPath) throws IOException {
        System.out.println("=== Merging Output Files ===");
        
        // 获取所有 part-r-* 文件
        FileStatus[] partFiles = fs.globStatus(new Path(outputPath, "part-r-*"));
        
        if (partFiles == null || partFiles.length == 0) {
            System.err.println("Warning: No part-r-* files found");
            return;
        }
        
        // 创建 words.txt 文件
        Path wordsPath = new Path(outputPath, "words.txt");
        org.apache.hadoop.fs.FSDataOutputStream wordsOut = fs.create(wordsPath);
        
        // MapReduce 的输出已经按字典序排序，直接合并即可
        for (FileStatus partFile : partFiles) {
            System.out.println("Merging file: " + partFile.getPath().getName());
            
            // 读取 part 文件内容
            org.apache.hadoop.fs.FSDataInputStream in = fs.open(partFile.getPath());
            byte[] buffer = new byte[4096];
            int bytesRead;
            
            while ((bytesRead = in.read(buffer)) > 0) {
                wordsOut.write(buffer, 0, bytesRead);
            }
            
            in.close();
        }
        
        wordsOut.close();
        System.out.println("words.txt file generated: " + wordsPath);
    }
}