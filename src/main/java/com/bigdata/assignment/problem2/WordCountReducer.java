package com.bigdata.assignment.problem2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 题目二：WordCount Reducer实现
 * 功能：接收来自Combiner的局部计数，进行最终聚合
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // 1. 初始化计数器为 0
        int sum = 0;
        
        // 2. 遍历来自 Combiner 的局部计数，累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        // 3. 将累加结果写入 result 变量
        result.set(sum);
        
        // 4. 输出最终的 (单词, 总计数) 结果
        context.write(key, result);
        
        // 5. 统计各分区的记录数
        int partitionId = getPartitionId(key.toString());
        context.getCounter("Partition Counters", "Partition " + partitionId + " Records").increment(1);
    }
    
    /**
     * 根据单词确定分区ID（与AlphabetPartitioner逻辑一致）
     */
    private int getPartitionId(String word) {
        if (word == null || word.isEmpty()) {
            return 0;
        }
        
        char firstChar = Character.toUpperCase(word.charAt(0));
        
        if (firstChar >= 'A' && firstChar <= 'F') {
            return 0;
        } else if (firstChar >= 'G' && firstChar <= 'N') {
            return 1;
        } else if (firstChar >= 'O' && firstChar <= 'S') {
            return 2;
        } else if (firstChar >= 'T' && firstChar <= 'Z') {
            return 3;
        } else {
            return 0;
        }
    }
}