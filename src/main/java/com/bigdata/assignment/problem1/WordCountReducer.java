package com.bigdata.assignment.problem1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 题目一：WordCount Reducer实现
 * 功能：接收相同单词的计数值，进行聚合统计
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // TODO: 实现单词计数的聚合逻辑
        // 1. 初始化计数器为 0
        int sum = 0;
        
        // 2. 遍历 values 中的所有计数值并累加
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        // 3. 将累加结果写入 result 变量
        result.set(sum);
        
        // 4. 输出最终的 (单词, 总计数) 结果
        context.write(key, result);
    }
}