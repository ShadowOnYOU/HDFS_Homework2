#!/bin/bash

# 题目三本地测试脚本 - 优化的WordCount实现
# 使用方法: ./run-problem3-local.sh <输入文件> <输出目录>

set -e  # 遇到错误时停止执行

# 检查参数
if [ $# -ne 2 ]; then
    echo "用法: $0 <输入文件> <输出目录>"
    echo "示例: $0 data/simple-test.txt output/problem3-simple"
    exit 1
fi

INPUT_FILE=$1
OUTPUT_DIR=$2
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
JAR_FILE="$PROJECT_ROOT/target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar"

echo "=== 题目三：优化的WordCount实现（本地测试）==="
echo "输入文件: $INPUT_FILE"
echo "输出目录: $OUTPUT_DIR"
echo "项目根目录: $PROJECT_ROOT"

# 检查输入文件是否存在
if [ ! -f "$PROJECT_ROOT/$INPUT_FILE" ]; then
    echo "错误: 输入文件不存在: $PROJECT_ROOT/$INPUT_FILE"
    exit 1
fi

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

# 清理输出目录（如果存在）
if [ -d "$PROJECT_ROOT/$OUTPUT_DIR" ]; then
    echo "清理输出目录: $PROJECT_ROOT/$OUTPUT_DIR"
    rm -rf "$PROJECT_ROOT/$OUTPUT_DIR"
fi

# 创建输出目录的父目录
mkdir -p "$(dirname "$PROJECT_ROOT/$OUTPUT_DIR")"

# 运行MapReduce作业（本地模式）
echo "开始运行题目三MapReduce作业（本地模式）..."
echo "主类: com.bigdata.assignment.problem3.WordCountOptimizedDriver"
echo "输入: $PROJECT_ROOT/$INPUT_FILE"
echo "输出: $PROJECT_ROOT/$OUTPUT_DIR"

cd "$PROJECT_ROOT"
hadoop jar "$JAR_FILE" \
    com.bigdata.assignment.problem3.WordCountOptimizedDriver \
    "$INPUT_FILE" \
    "$OUTPUT_DIR"

# 检查作业是否成功
if [ $? -eq 0 ]; then
    echo ""
    echo "=== 作业执行成功 ==="
    
    # 显示输出目录内容
    echo "输出目录内容:"
    ls -la "$OUTPUT_DIR/"
    
    # 显示部分结果
    echo ""
    echo "=== 词频统计结果（前20行）==="
    if [ -f "$OUTPUT_DIR/part-r-00000" ]; then
        head -20 "$OUTPUT_DIR/part-r-00000"
    else
        echo "结果文件不存在"
        ls -la "$OUTPUT_DIR/"
    fi
    
    echo ""
    echo "题目三执行完成！"
    echo "结果路径: $PROJECT_ROOT/$OUTPUT_DIR"
    
else
    echo "=== 作业执行失败 ==="
    echo "请检查错误日志并重新运行"
    exit 1
fi