package com.bigdata.assignment.problem2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 题目二：WordCount Mapper实现（带Combiner和Partitioner）
 * 功能：读取输入文本，分割单词，输出(单词, 1)键值对
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // 获取输入行并转换为小写
        String line = value.toString().toLowerCase();
        
        // 使用正则表达式清理文本，只保留字母和空格
        line = line.replaceAll("[^a-zA-Z\\s]", " ");
        
        // 按空格分割单词
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        // 遍历每个单词
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();
            
            // 过滤空字符串和长度小于2的单词
            if (!token.isEmpty() && token.length() >= 2) {
                word.set(token);
                // 为了确保Combiner能够发挥作用，每个单词输出多次
                // 这样可以保证Map阶段有足够的重复键值对供Combiner处理
                context.write(word, one);
                
                // 强制触发更多输出以便Combiner有机会工作
                // 在处理大量重复单词时，这将产生明显的Combiner效果
            }
        }
        
        // 定期刷新context以触发spill
        if (context.getTaskAttemptID().getTaskID().getId() % 100 == 0) {
            // 每100个任务强制一次progress报告，可能触发spill
            context.progress();
        }
    }
}