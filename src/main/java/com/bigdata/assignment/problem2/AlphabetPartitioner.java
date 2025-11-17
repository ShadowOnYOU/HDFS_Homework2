package com.bigdata.assignment.problem2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 题目二：按字母分区的Partitioner实现
 * 功能：按单词首字母进行分区，实现负载均衡
 * 
 * 分区规则：
 * - A-F 字母开头的单词分配到分区 0
 * - G-N 字母开头的单词分配到分区 1  
 * - O-S 字母开头的单词分配到分区 2
 * - T-Z 字母开头的单词分配到分区 3
 * 
 * 字符处理规则：
 * - 单词首字母统一转换为大写后进行分区判断
 * - 数字开头的单词（0-9）分配到分区 0
 * - 特殊字符开头的单词（非字母非数字）分配到分区 0
 * - 空字符串或 null 值分配到分区 0
 */
public class AlphabetPartitioner extends Partitioner<Text, IntWritable> {
    
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        
        // TODO: 实现按字母分区的逻辑
        // 边界情况处理：空值或空字符串
        if (key == null || key.toString().isEmpty()) {
            return 0; // 分配到分区0
        }
        
        // 1. 获取单词的首字母并转换为大写
        String word = key.toString();
        char firstChar = Character.toUpperCase(word.charAt(0));
        
        // 2. 按照作业要求的分区规则
        // A-F → 分区0, G-N → 分区1, O-S → 分区2, T-Z → 分区3
        if (firstChar >= 'A' && firstChar <= 'F') {
            // A-F → 分区0
            return 0;
        } else if (firstChar >= 'G' && firstChar <= 'N') {
            // G-N → 分区1
            return 1;
        } else if (firstChar >= 'O' && firstChar <= 'S') {
            // O-S → 分区2
            return 2;
        } else if (firstChar >= 'T' && firstChar <= 'Z') {
            // T-Z → 分区3
            return 3;
        } else {
            // 3. 处理边界情况：数字和特殊字符
            // 数字开头的单词（0-9）分配到分区 0
            // 特殊字符开头的单词（非字母非数字）分配到分区 0
            // 空字符串或 null 值分配到分区 0
            return 0;
        }
    }
}