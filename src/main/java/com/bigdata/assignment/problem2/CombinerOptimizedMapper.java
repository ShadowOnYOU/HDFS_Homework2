package com.bigdata.assignment.problem2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 优化版本的Mapper，在Map端进行预聚合以确保Combiner有足够的数据处理
 */
public class CombinerOptimizedMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable count = new IntWritable();
    private Text word = new Text();
    
    // 使用内存中的HashMap进行Map端预聚合
    private Map<String, Integer> localWordCount = new HashMap<>();
    private static final int FLUSH_THRESHOLD = 1000; // 每1000个不同单词就输出一次
    
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
                // 进行Map端预聚合
                localWordCount.put(token, localWordCount.getOrDefault(token, 0) + 1);
                
                // 当达到阈值时，输出当前累积的计数
                if (localWordCount.size() >= FLUSH_THRESHOLD) {
                    flushLocalCounts(context);
                }
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 在Map任务结束时输出剩余的计数
        flushLocalCounts(context);
        super.cleanup(context);
    }
    
    /**
     * 输出本地累积的单词计数
     */
    private void flushLocalCounts(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : localWordCount.entrySet()) {
            word.set(entry.getKey());
            count.set(entry.getValue());
            context.write(word, count);
        }
        localWordCount.clear();
        
        // 强制触发进度更新，可能触发spill
        context.progress();
    }
}