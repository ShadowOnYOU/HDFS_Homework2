#!/bin/bash

# Problem2 强制启用Combiner版本运行脚本

echo "=== 编译Problem2优化版本 ==="
cd /Users/shadowonyou/2025研一上/大数据理论与实践/HDFS_Homework2
mvn clean compile

if [ $? -ne 0 ]; then
    echo "编译失败！"
    exit 1
fi

echo "=== 打包JAR文件 ==="
mvn package -DskipTests

if [ $? -ne 0 ]; then
    echo "打包失败！"
    exit 1
fi

echo "=== 上传JAR到远程服务器 ==="
scp target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar hadoop@172.19.240.185:~/hadoop-jars/

if [ $? -ne 0 ]; then
    echo "上传失败！"
    exit 1
fi

echo "=== 执行Problem2优化版本 ==="
ssh hadoop@172.19.240.185 << 'EOF'
    # 设置Hadoop环境变量
    export JAVA_HOME=/opt/jdk1.8.0_202
    export HADOOP_HOME=/opt/hadoop-3.4.2
    export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
    
    # 使用更激进的配置来强制启用Combiner
    hadoop jar ~/hadoop-jars/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
        com.bigdata.assignment.problem2.WordCountWithCombinerDriver \
        /user/522025320139/big-input \
        /user/522025320139/homework1/problem2-optimized \
        -D mapreduce.map.sort.spill.percent=0.05 \
        -D mapreduce.task.io.sort.mb=20 \
        -D mapreduce.map.combine.minspills=1 \
        -D mapreduce.task.io.sort.factor=5 \
        -D mapreduce.map.output.compress=true \
        -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
        -D mapreduce.task.spill.sort.threads=1 \
        -D mapreduce.reduce.input.buffer.percent=0.0
EOF

echo "=== 下载结果文件 ==="
scp hadoop@172.19.240.185:/tmp/problem2-optimized-output.txt result/2-optimized.txt

echo "Problem2优化版本执行完成！结果已保存到 result/2-optimized.txt"