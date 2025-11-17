package com.bigdata.assignment.problem3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 题目三：WordCount 优化版Combiner实现
 * 功能：Map端本地聚合，减少网络传输，支持性能监控
 */
public class WordCountOptimizedCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // TODO: 实现 Combiner 方法
        // 1. 初始化计数器为 0
        int sum = 0;
        int inputCount = 0;
        
        // 2. 遍历相同单词的计数值，累加求和
        for (IntWritable value : values) {
            sum += value.get();
            inputCount++;
        }
        
        // 3. 输出键值对：(单词, 局部计数)
        result.set(sum);
        context.write(key, result);
        
        // 4. 添加计数器统计 Combiner 的输入输出记录数
        context.getCounter("Combiner Performance", "Input Records").increment(inputCount);
        context.getCounter("Combiner Performance", "Output Records").increment(1);
        
        // 统计数据压缩效果
        if (inputCount > 1) {
            context.getCounter("Combiner Performance", "Records Compressed").increment(inputCount - 1);
        }
    }
}