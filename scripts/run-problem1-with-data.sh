#!/bin/bash

# 题目一运行脚本 - 使用上传的测试数据
# 使用方法: ./run-problem1-with-data.sh <学号> <测试文件名>

set -e  # 遇到错误时停止执行

# 检查参数
if [ $# -ne 2 ]; then
    echo "用法: $0 <学号> <测试文件名>"
    echo "示例: $0 2021001001 simple-test.txt"
    echo "可用的测试文件:"
    echo "  - simple-test.txt"
    echo "  - alice-in-wonderland.txt"
    echo "  - pride-and-prejudice.txt"
    exit 1
fi

STUDENT_ID=$1
TEST_FILE=$2
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
JAR_FILE="$PROJECT_ROOT/target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar"

echo "=== 题目一：HDFS操作与WordCount实现 ==="
echo "学号: $STUDENT_ID"
echo "测试文件: $TEST_FILE"
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
INPUT_PATH="/$STUDENT_ID/test-data/$TEST_FILE"
OUTPUT_PATH="/$STUDENT_ID/homework1/problem1-$(basename "$TEST_FILE" .txt)"

echo "输入路径: $INPUT_PATH"
echo "输出路径: $OUTPUT_PATH"

# 检查输入文件
echo "检查输入文件..."
if ! hdfs dfs -test -e "$INPUT_PATH"; then
    echo "错误: 输入文件不存在: $INPUT_PATH"
    echo "请先运行 ./scripts/upload-test-data.sh $STUDENT_ID 上传测试数据"
    exit 1
fi

# 显示输入文件信息
echo "输入文件信息:"
hdfs dfs -ls -h "$INPUT_PATH"

# 清理输出目录（如果存在）
echo "清理输出目录..."
hdfs dfs -rm -r -f "$OUTPUT_PATH"

# 运行MapReduce作业
echo "开始运行题目一MapReduce作业..."
echo "主类: com.bigdata.assignment.problem1.WordCountDriver"

hadoop jar "$JAR_FILE" \
    com.bigdata.assignment.problem1.WordCountDriver \
    "$INPUT_PATH" \
    "$OUTPUT_PATH"

# 检查作业是否成功
if [ $? -eq 0 ]; then
    echo "=== 作业执行成功 ==="
    
    # 显示输出目录内容
    echo "输出目录内容:"
    hdfs dfs -ls "$OUTPUT_PATH"
    
    # 显示部分结果
    echo ""
    echo "=== 词频统计结果（前20行）==="
    hdfs dfs -cat "$OUTPUT_PATH/part-r-*" | head -20
    
    # 显示统计信息
    echo ""
    echo "=== 统计信息 ==="
    hdfs dfs -cat "$OUTPUT_PATH/statistics.txt" 2>/dev/null || echo "统计信息文件不存在"
    
    # 保存结果到本地
    LOCAL_OUTPUT="$PROJECT_ROOT/output/problem1-$(basename "$TEST_FILE" .txt)"
    mkdir -p "$LOCAL_OUTPUT"
    
    echo "保存结果到本地: $LOCAL_OUTPUT"
    hdfs dfs -get "$OUTPUT_PATH/*" "$LOCAL_OUTPUT/" 2>/dev/null || true
    
    echo "题目一执行完成！"
    echo "HDFS结果路径: $OUTPUT_PATH"
    echo "本地结果路径: $LOCAL_OUTPUT"
    
else
    echo "=== 作业执行失败 ==="
    echo "请检查错误日志并重新运行"
    exit 1
fi