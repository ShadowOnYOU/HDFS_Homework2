#!/bin/bash

echo "=== 验证远程JAR文件 ==="
echo ""
echo "1. 检查本地JAR文件时间戳："
ls -lh target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar

echo ""
echo "2. 检查远程JAR文件时间戳："
ssh hadoop@172.19.240.185 "ls -lh ~/hadoop-jars/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar"

echo ""
echo "3. 检查JAR中是否包含新的Driver类（带调试输出）："
jar tf target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar | grep "WordCountWithCombinerDriver.class"

echo ""
echo "4. 查看Driver类的字节码大小（如果大小很小说明没有调试代码）："
unzip -l target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar | grep "WordCountWithCombinerDriver.class"

echo ""
echo "=== 测试运行（会立即看到调试输出） ==="
echo "现在SSH到远程机器运行Problem2，你应该立即看到调试输出："
echo ""
echo "命令："
echo "ssh hadoop@172.19.240.185"
echo "hadoop jar ~/hadoop-jars/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar com.bigdata.assignment.problem2.WordCountWithCombinerDriver /user/522025320139/big-input /user/522025320139/homework1/problem2"
