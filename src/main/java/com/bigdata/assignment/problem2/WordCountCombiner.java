package com.bigdata.assignment.problem2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 题目二：WordCount Combiner实现
 * 功能：在Map端进行本地聚合，减少Shuffle阶段的网络传输数据量
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();
    private static boolean initialized = false;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // setup 保持轻量：只使用计数器记录初始化（避免大量 stdout）
        if (!initialized) {
            context.getCounter("Combiner Status", "Combiner Setup Called").increment(1);
            initialized = true;
        }
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // 增加Combiner执行标记
        context.getCounter("Combiner Status", "Combiner Invoked").increment(1);
        
        // 避免频繁打印到 stdout，会造成日志泛滥并影响性能/可读性。
        // 通过自定义计数器来监控 Combiner 的调用次数和有效合并情况。
        
        // TODO: 实现 Combiner 逻辑
        // 1. 初始化计数器为 0
        int sum = 0;
        int inputCount = 0;
        
        // 2. 遍历 values 中的所有计数值并累加
        for (IntWritable value : values) {
            sum += value.get();
            inputCount++;
            // 统计Combiner输入记录数
            context.getCounter("Combiner Status", "Combiner Input Records").increment(1);
        }
        
        // 只有当输入记录数大于1时才说明Combiner真正起作用
        if (inputCount > 1) {
            context.getCounter("Combiner Status", "Effective Combinations").increment(1);
            context.getCounter("Combiner Status", "Records Reduced").increment(inputCount - 1);
            
            // 若需要查看少量示例，可在调试时临时开启打印，或直接查看 container 日志。
        }
        
        // 3. 将累加结果写入 result 变量
        result.set(sum);
        
        // 4. 输出 (单词, 局部计数) 键值对
        context.write(key, result);
        
        // 5. 统计Combiner输出记录数
        context.getCounter("Combiner Status", "Combiner Output Records").increment(1);
        
        // 记录单词频率分布
        if (sum >= 100) {
            context.getCounter("Word Frequency", "High Frequency Words (>=100)").increment(1);
        } else if (sum >= 10) {
            context.getCounter("Word Frequency", "Medium Frequency Words (10-99)").increment(1);
        } else {
            context.getCounter("Word Frequency", "Low Frequency Words (1-9)").increment(1);
        }
    }
}