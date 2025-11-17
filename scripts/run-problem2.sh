#!/bin/bash

# 题目二运行脚本 - 自定义Combiner和Partitioner
# 使用方法: ./run-problem2.sh [学号]

set -e  # 遇到错误时停止执行

# 检查参数
if [ $# -ne 1 ]; then
    echo "用法: $0 <学号>"
    echo "示例: $0 2021001001"
    exit 1
fi

STUDENT_ID=$1
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
JAR_FILE="$PROJECT_ROOT/target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar"

echo "=== 题目二：自定义Combiner和Partitioner ==="
echo "学号: $STUDENT_ID"
echo "项目根目录: $PROJECT_ROOT"

# 编译项目
echo "正在编译项目..."
cd "$PROJECT_ROOT"
mvn clean package -DskipTests

# 检查JAR文件是否存在
if [ ! -f "$JAR_FILE" ]; then
    echo "错误: JAR文件不存在: $JAR_FILE"
    echo "请确保Maven编译成功"
    exit 1
fi

echo "编译完成: $JAR_FILE"

# 设置路径
INPUT_PATH="/public/data/wordcount"
OUTPUT_PATH="/$STUDENT_ID/homework1/problem2"

echo "输入路径: $INPUT_PATH"
echo "输出路径: $OUTPUT_PATH"

# 检查输入目录
echo "检查输入目录..."
if ! hdfs dfs -test -d "$INPUT_PATH"; then
    echo "错误: 输入目录不存在: $INPUT_PATH"
    echo "请确保HDFS上存在公共数据目录"
    exit 1
fi

# 显示输入文件信息
echo "输入目录内容:"
hdfs dfs -ls "$INPUT_PATH"

# 清理输出目录（如果存在）
echo "清理输出目录..."
hdfs dfs -rm -r -f "$OUTPUT_PATH"

# 创建个人目录（如果不存在）
hdfs dfs -mkdir -p "/$STUDENT_ID/homework1"

# 运行MapReduce作业
echo "开始运行题目二MapReduce作业（带Combiner和Partitioner）..."
echo "主类: com.bigdata.assignment.problem2.WordCountWithCombinerDriver"
echo "特性: 启用Combiner优化和按字母分区"

hadoop jar "$JAR_FILE" \
    com.bigdata.assignment.problem2.WordCountWithCombinerDriver \
    "$INPUT_PATH" \
    "$OUTPUT_PATH"

# 检查作业是否成功
if [ $? -eq 0 ]; then
    echo "=== 作业执行成功 ==="
    
    # 显示输出目录内容
    echo "输出目录内容:"
    hdfs dfs -ls "$OUTPUT_PATH"
    
    # 显示各分区结果概览
    echo ""
    echo "=== 分区结果概览 ==="
    for i in {0..3}; do
        if hdfs dfs -test -f "$OUTPUT_PATH/part-r-0000$i"; then
            echo "分区 $i (part-r-0000$i):"
            hdfs dfs -cat "$OUTPUT_PATH/part-r-0000$i" | wc -l | awk '{print "  单词数量: " $1}'
            hdfs dfs -cat "$OUTPUT_PATH/part-r-0000$i" | head -5 | awk '{print "  示例: " $0}'
        fi
    done
    
    # 显示统计信息
    echo ""
    echo "=== Combiner和分区统计信息 ==="
    hdfs dfs -cat "$OUTPUT_PATH/statistics.txt" 2>/dev/null || echo "统计信息文件不存在"
    
    # 合并所有分区结果进行词频排序
    echo ""
    echo "=== 词频统计结果（按频次降序前20） ==="
    hdfs dfs -cat "$OUTPUT_PATH/part-r-*" | \
    sort -k2,2nr -k1,1 | \
    head -20 | \
    awk '{printf "%-20s %s\n", $1, $2}'
    
    # 保存结果到本地
    LOCAL_OUTPUT="$PROJECT_ROOT/output/problem2"
    mkdir -p "$LOCAL_OUTPUT"
    
    echo "保存结果到本地: $LOCAL_OUTPUT"
    hdfs dfs -get "$OUTPUT_PATH/*" "$LOCAL_OUTPUT/" 2>/dev/null || true
    
    # 生成本地合并的词频文件
    echo "生成合并的词频统计文件..."
    if [ -f "$LOCAL_OUTPUT/part-r-00000" ]; then
        cat "$LOCAL_OUTPUT"/part-r-* | sort -k2,2nr -k1,1 > "$LOCAL_OUTPUT/words.txt"
        echo "词频文件已保存: $LOCAL_OUTPUT/words.txt"
    fi
    
    echo "题目二执行完成！"
    echo "HDFS结果路径: $OUTPUT_PATH"
    echo "本地结果路径: $LOCAL_OUTPUT"
    echo "特色功能:"
    echo "  - Combiner优化: 减少网络传输"
    echo "  - 按字母分区: A-F(0), G-N(1), O-S(2), T-Z(3)"
    
else
    echo "=== 作业执行失败 ==="
    echo "请检查错误日志并重新运行"
    exit 1
fi