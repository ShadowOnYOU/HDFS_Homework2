package com.bigdata.assignment.problem3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 题目三：WordCount 优化版Mapper实现
 * 功能：处理大规模文本数据，支持性能监控和调优
 */
public class WordCountOptimizedMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        try {
            // TODO: 实现优化的 map 方法
            // 1. 将输入行转换为小写并分割为单词
            String line = value.toString().toLowerCase();
            
            // 2. 过滤非字母字符，只保留有效单词
            line = line.replaceAll("[^a-zA-Z\\s]", " ");
            
            // 3. 按空格分词
            StringTokenizer tokenizer = new StringTokenizer(line);
            
            // 4. 添加计数器统计处理的单词数和行数
            context.getCounter("Custom Counters", "Lines Processed").increment(1);
            
            // 5. 遍历每个单词并输出
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().trim();
                
                // 过滤长度小于2的单词
                if (!token.isEmpty() && token.length() >= 2) {
                    word.set(token);
                    context.write(word, one);
                    
                    // 统计处理的单词数
                    context.getCounter("Custom Counters", "Words Processed").increment(1);
                }
            }
            
        } catch (Exception e) {
            // 6. 异常处理：跳过无效行并记录日志
            System.err.println("处理行时发生错误: " + e.getMessage());
            context.getCounter("Custom Counters", "Error Lines").increment(1);
        }
    }
}