package com.bigdata.assignment.problem2;

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
 * 题目二：带Combiner和Partitioner的WordCount Driver
 * 功能：配置和启动带优化功能的MapReduce作业
 */
public class WordCountWithCombinerDriver {
    
    public static void main(String[] args) throws Exception {
        
        // 调试确认：打印类名
        System.err.println("=== 执行类: WordCountWithCombinerDriver ===");
        
        // 检查命令行参数
        if (args.length != 2) {
            System.err.println("Error: Got " + args.length + " parameters, need 2");
            System.err.println("Usage: WordCountWithCombinerDriver <input> <output>");
            System.err.println("Example: WordCountWithCombinerDriver /user/xxx/input /user/xxx/output");
            System.exit(-1);
        }
        
        // TODO: 创建 Configuration 和 Job 对象
        Configuration conf = new Configuration();
        
    // 优化的Combiner配置，确保Combiner被充分利用
    System.out.println("=== Configuring Combiner Parameters ===");
    conf.setFloat("mapreduce.map.sort.spill.percent", 0.8f); // 80%时spill，较标准
    conf.setInt("mapreduce.task.io.sort.mb", 100); // 排序缓冲区100MB
    conf.setInt("mapreduce.map.combine.minspills", 1); // 只要有spill就运行combiner，最大化Combiner效果
    conf.setInt("mapreduce.task.io.sort.factor", 10); // merge factor
    conf.setBoolean("mapreduce.map.speculative", false); // 关闭推测执行
        
    System.out.println("Spill threshold: 80%");
    System.out.println("Sort buffer: 100MB");
    System.out.println("Min spills: 1 (optimized to ensure Combiner runs fully)");
    System.out.println("=====================================");
        
        Job job = Job.getInstance(conf, "word count with combiner and partitioner");
        
        // TODO: 设置基本 Job 参数
        // 1. 设置 JAR 文件
        job.setJarByClass(WordCountWithCombinerDriver.class);
        
        // 2. 设置 Mapper、Combiner 和 Reducer 类
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(WordCountWithCombinerReducer.class);
        
        // 3. 设置 Partitioner 类
        job.setPartitionerClass(AlphabetPartitioner.class);
        
        // 4. 设置输出键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 5. 设置 Reduce 任务数量为 4（对应 4 个分区）
        job.setNumReduceTasks(4);
        
        // TODO: 实现 HDFS 操作
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        
        // 1. 检查输入目录是否存在
        Path inputPath = new Path(args[0]);
        if (!fs.exists(inputPath)) {
            System.err.println("Input directory not found: " + args[0]);
            System.exit(-1);
        }
        
        System.out.println("Input directory check passed: " + args[0]);
        
        // 显示输入目录信息
        System.out.println("=== Input Directory Information ===");
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
        
        // 确认Combiner配置
        System.out.println("=== Job Configuration Confirmation ===");
        System.out.println("Mapper class: " + job.getMapperClass().getSimpleName());
        System.out.println("Combiner class: " + job.getCombinerClass().getSimpleName());
        System.out.println("Reducer class: " + job.getReducerClass().getSimpleName());
        System.out.println("Partitioner class: " + job.getPartitionerClass().getSimpleName());
        System.out.println("Reduce tasks: " + job.getNumReduceTasks());
        System.out.println("=====================================");
        
        // 记录开始时间
        long startTime = System.currentTimeMillis();
        
        // TODO: 提交作业并等待完成
        // 使用 job.waitForCompletion(true)
        boolean success = job.waitForCompletion(true);
        
        long endTime = System.currentTimeMillis();
        
        if (success) {
            System.out.println("Job execution successful!");
            
            // 获取作业统计信息
            Counters counters = job.getCounters();
            
            // 输出执行统计
            System.out.println("=== Job Execution Statistics ===");
            System.out.println("Processing time: " + (endTime - startTime) + " ms");
            
            // 获取输入输出记录数等统计信息
            long mapInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
            long mapOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
            long reduceInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS").getValue();
            long reduceOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue();
            long combineInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_INPUT_RECORDS").getValue();
            long combineOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_OUTPUT_RECORDS").getValue();
            
            // 获取自定义Combiner统计信息
            long combinerInvoked = counters.findCounter("Combiner Status", "Combiner Invoked").getValue();
            long combinerInputCustom = counters.findCounter("Combiner Status", "Combiner Input Records").getValue();
            long combinerOutputCustom = counters.findCounter("Combiner Status", "Combiner Output Records").getValue();
            long effectiveCombinations = counters.findCounter("Combiner Status", "Effective Combinations").getValue();
            long recordsReduced = counters.findCounter("Combiner Status", "Records Reduced").getValue();
            
            System.out.println("Input files: " + fileCount);
            System.out.println("Input records: " + mapInputRecords);
            System.out.println("Total words: " + mapOutputRecords);
            System.out.println("Unique words: " + reduceOutputRecords);
            
            System.out.println("\n=== Combiner Execution Statistics ===");
            System.out.println("System stats - Combine input records: " + combineInputRecords);
            System.out.println("System stats - Combine output records: " + combineOutputRecords);
            System.out.println("Custom stats - Combiner invocations: " + combinerInvoked);
            System.out.println("Custom stats - Combiner input records: " + combinerInputCustom);
            System.out.println("Custom stats - Combiner output records: " + combinerOutputCustom);
            System.out.println("Custom stats - Effective combinations: " + effectiveCombinations);
            System.out.println("Custom stats - Records reduced: " + recordsReduced);
            
            // 输出分区统计
            System.out.println("=== Partition Statistics ===");
            for (int i = 0; i < 4; i++) {
                long partitionRecords = counters.findCounter("Partition Counters", "Partition " + i + " Records").getValue();
                System.out.println("Partition " + i + " records: " + partitionRecords);
            }
            
            // 计算Combiner效率
            if (combineInputRecords > 0) {
                double efficiency = (1.0 - (double)combineOutputRecords / combineInputRecords) * 100;
                System.out.println("System stats - Combiner compression rate: " + String.format("%.2f", efficiency) + "%");
            } else {
                System.out.println("WARNING: Combiner did not execute!");
            }
            
            if (combinerInputCustom > 0) {
                double customEfficiency = (1.0 - (double)combinerOutputCustom / combinerInputCustom) * 100;
                System.out.println("Custom stats - Combiner compression rate: " + String.format("%.2f", customEfficiency) + "%");
                
                if (effectiveCombinations > 0) {
                    double avgReduction = (double)recordsReduced / effectiveCombinations;
                    System.out.println("Average records reduced per effective combination: " + String.format("%.2f", avgReduction));
                }
            }
            
            System.out.println("\nResults saved to: " + args[1]);
            System.out.println("Main result files:");
            for (int i = 0; i < 4; i++) {
                System.out.println("  Partition " + i + ": " + args[1] + "/part-r-0000" + i);
            }
            
            // 生成 words.txt 和 statistics.txt
            generateOutputFiles(fs, outputPath, counters, endTime - startTime, fileCount);
            
        } else {
            System.err.println("Job execution failed!");
            System.exit(-1);
        }
    }
    
    /**
     * 生成 words.txt 和 statistics.txt 文件
     */
    private static void generateOutputFiles(FileSystem fs, Path outputPath, Counters counters, 
                                           long processingTime, int fileCount) throws IOException {
        System.out.println("\n=== Generating Output Files ===");
        
        // 1. 合并所有 part-r-* 文件为 words.txt
        Path wordsPath = new Path(outputPath, "words.txt");
        org.apache.hadoop.fs.FSDataOutputStream wordsOut = fs.create(wordsPath);
        
        // 按顺序合并 4 个分区文件
        for (int i = 0; i < 4; i++) {
            Path partFile = new Path(outputPath, String.format("part-r-0000%d", i));
            if (fs.exists(partFile)) {
                System.out.println("Merging partition file: " + partFile.getName());
                org.apache.hadoop.fs.FSDataInputStream in = fs.open(partFile);
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) > 0) {
                    wordsOut.write(buffer, 0, bytesRead);
                }
                in.close();
            }
        }
        wordsOut.close();
        System.out.println("words.txt file generated");
        
        // 2. 生成 statistics.txt
        Path statisticsPath = new Path(outputPath, "statistics.txt");
        org.apache.hadoop.fs.FSDataOutputStream statsOut = fs.create(statisticsPath);
        
        // 获取所有需要的统计数据
        long mapOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
        long reduceOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue();
        long combineInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_INPUT_RECORDS").getValue();
        long combineOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_OUTPUT_RECORDS").getValue();
        
        // 构建统计信息
        StringBuilder stats = new StringBuilder();
        stats.append("input_files\t").append(fileCount).append("\n");
        stats.append("processing_time\t").append(processingTime).append("\n");
        stats.append("total_words\t").append(mapOutputRecords).append("\n");
        stats.append("unique_words\t").append(reduceOutputRecords).append("\n");
        stats.append("combiner_input_records\t").append(combineInputRecords).append("\n");
        stats.append("combiner_output_records\t").append(combineOutputRecords).append("\n");
        
        // 添加分区统计
        for (int i = 0; i < 4; i++) {
            long partitionRecords = counters.findCounter("Partition Counters", "Partition " + i + " Records").getValue();
            stats.append(String.format("partition_%d_records\t%d\n", i, partitionRecords));
        }
        
        statsOut.writeBytes(stats.toString());
        statsOut.close();
        System.out.println("statistics.txt file generated");
        System.out.println("=====================================");
    }
}
