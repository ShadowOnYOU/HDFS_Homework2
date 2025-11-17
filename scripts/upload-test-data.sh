#!/bin/bash

# 数据上传脚本 - 将本地测试数据上传到HDFS
# 使用方法: ./upload-test-data.sh <学号>

set -e  # 遇到错误时停止执行

# 检查参数
if [ $# -ne 1 ]; then
    echo "用法: $0 <学号>"
    echo "示例: $0 2021001001"
    exit 1
fi

STUDENT_ID=$1
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
LOCAL_DATA_DIR="$PROJECT_ROOT/data"

echo "=== 上传测试数据到HDFS ==="
echo "学号: $STUDENT_ID"
echo "本地数据目录: $LOCAL_DATA_DIR"

# 检查本地数据目录是否存在
if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo "错误: 本地数据目录不存在: $LOCAL_DATA_DIR"
    exit 1
fi

# 显示本地数据文件
echo "本地测试文件:"
ls -la "$LOCAL_DATA_DIR"/*.txt

# HDFS路径设置
HDFS_BASE_DIR="/$STUDENT_ID"
HDFS_DATA_DIR="$HDFS_BASE_DIR/test-data"

echo "HDFS基础目录: $HDFS_BASE_DIR"
echo "HDFS数据目录: $HDFS_DATA_DIR"

# 创建HDFS目录结构
echo "创建HDFS目录结构..."
hdfs dfs -mkdir -p "$HDFS_BASE_DIR"
hdfs dfs -mkdir -p "$HDFS_DATA_DIR"
hdfs dfs -mkdir -p "$HDFS_BASE_DIR/homework1"

# 清理已存在的测试数据
echo "清理已存在的测试数据..."
hdfs dfs -rm -r -f "$HDFS_DATA_DIR/*" 2>/dev/null || true

# 上传测试文件
echo "上传测试文件到HDFS..."
for file in "$LOCAL_DATA_DIR"/*.txt; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        echo "上传: $filename"
        hdfs dfs -put "$file" "$HDFS_DATA_DIR/"
    fi
done

# 验证上传结果
echo ""
echo "=== 验证上传结果 ==="
echo "HDFS数据目录内容:"
hdfs dfs -ls "$HDFS_DATA_DIR"

echo ""
echo "文件详细信息:"
hdfs dfs -ls -h "$HDFS_DATA_DIR"

echo ""
echo "=== 数据上传完成 ==="
echo "你现在可以使用以下命令运行MapReduce作业："
echo ""
echo "题目一（使用simple-test.txt）："
echo "./scripts/run-problem1-with-data.sh $STUDENT_ID simple-test.txt"
echo ""
echo "题目一（使用alice-in-wonderland.txt）："
echo "./scripts/run-problem1-with-data.sh $STUDENT_ID alice-in-wonderland.txt"
echo ""
echo "题目一（使用pride-and-prejudice.txt）："
echo "./scripts/run-problem1-with-data.sh $STUDENT_ID pride-and-prejudice.txt"