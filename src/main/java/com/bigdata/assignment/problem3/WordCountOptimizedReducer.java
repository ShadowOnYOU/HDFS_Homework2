package com.bigdata.assignment.problem3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 题目三：WordCount 优化版Reducer实现
 * 功能：最终聚合统计，支持性能监控和结果排序
 */
public class WordCountOptimizedReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // TODO: 实现 Reducer 方法
        // 1. 初始化计数器为 0
        int sum = 0;
        
        // 2. 遍历来自 Combiner 的局部计数，累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        // 3. 输出最终结果：(单词, 总计数)
        result.set(sum);
        context.write(key, result);
        
        // 4. 添加计数器统计最终输出的单词数
        context.getCounter("Final Results", "Unique Words").increment(1);
        context.getCounter("Final Results", "Total Word Count").increment(sum);
    }
}