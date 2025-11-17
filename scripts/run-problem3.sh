#!/bin/bash

# 题目三运行脚本 - MapReduce任务调优与性能分析
# 使用方法: ./run-problem3.sh [学号]

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

echo "=== 题目三：MapReduce任务调优与性能分析 ==="
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
OUTPUT_PATH="/$STUDENT_ID/homework1/problem3"

echo "输入路径: $INPUT_PATH"
echo "输出路径: $OUTPUT_PATH"

# 检查输入目录
echo "检查输入目录..."
if ! hdfs dfs -test -d "$INPUT_PATH"; then
    echo "错误: 输入目录不存在: $INPUT_PATH"
    echo "请确保HDFS上存在公共数据目录"
    exit 1
fi

# 显示输入文件信息和数据规模
echo ""
echo "=== 输入数据分析 ==="
hdfs dfs -ls "$INPUT_PATH"
echo ""
echo "数据规模统计:"
hdfs dfs -du -h "$INPUT_PATH"

# 清理输出目录（如果存在）
echo "清理输出目录..."
hdfs dfs -rm -r -f "$OUTPUT_PATH"

# 创建个人目录（如果不存在）
hdfs dfs -mkdir -p "/$STUDENT_ID/homework1"

# 显示Hadoop集群资源信息
echo ""
echo "=== Hadoop集群资源信息 ==="
yarn node -list -states RUNNING 2>/dev/null | head -10 || echo "无法获取节点信息"

# 运行MapReduce作业
echo ""
echo "=== 开始性能优化的MapReduce作业 ==="
echo "主类: com.bigdata.assignment.problem3.WordCountOptimizedDriver"
echo "优化特性:"
echo "  - Map任务并行度优化"
echo "  - Reduce任务数量: 4"
echo "  - 启用Combiner本地聚合"
echo "  - 内存配置优化"
echo "  - 详细性能监控"

# 记录开始时间
START_TIME=$(date +%s)
echo "开始时间: $(date)"

hadoop jar "$JAR_FILE" \
    com.bigdata.assignment.problem3.WordCountOptimizedDriver \
    "$INPUT_PATH" \
    "$OUTPUT_PATH"

# 记录结束时间
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

# 检查作业是否成功
if [ $? -eq 0 ]; then
    echo ""
    echo "=== 作业执行成功 ==="
    echo "结束时间: $(date)"
    echo "总执行时间: ${EXECUTION_TIME} 秒"
    
    # 显示输出目录内容
    echo ""
    echo "输出目录内容:"
    hdfs dfs -ls "$OUTPUT_PATH"
    
    # 显示性能报告
    echo ""
    echo "=== 详细性能报告 ==="
    hdfs dfs -cat "$OUTPUT_PATH/performance-report.txt" 2>/dev/null || echo "性能报告文件不存在"
    
    # 显示词频统计结果概览
    echo ""
    echo "=== 词频统计结果（按频次降序前20） ==="
    hdfs dfs -cat "$OUTPUT_PATH/part-r-*" | \
    sort -k2,2nr -k1,1 | \
    head -20 | \
    awk '{printf "%-20s %s\n", $1, $2}'
    
    # 统计各分区的数据量
    echo ""
    echo "=== 分区负载分析 ==="
    for i in {0..3}; do
        if hdfs dfs -test -f "$OUTPUT_PATH/part-r-0000$i"; then
            RECORDS=$(hdfs dfs -cat "$OUTPUT_PATH/part-r-0000$i" | wc -l)
            SIZE=$(hdfs dfs -du "$OUTPUT_PATH/part-r-0000$i" | awk '{print $1}')
            echo "分区 $i: $RECORDS 个单词, $SIZE 字节"
        fi
    done
    
    # 保存结果到本地
    LOCAL_OUTPUT="$PROJECT_ROOT/output/problem3"
    mkdir -p "$LOCAL_OUTPUT"
    
    echo ""
    echo "保存结果到本地: $LOCAL_OUTPUT"
    hdfs dfs -get "$OUTPUT_PATH/*" "$LOCAL_OUTPUT/" 2>/dev/null || true
    
    # 生成合并的词频文件（按频次降序）
    echo "生成优化的词频统计文件..."
    if [ -f "$LOCAL_OUTPUT/part-r-00000" ]; then
        cat "$LOCAL_OUTPUT"/part-r-* | \
        sort -k2,2nr -k1,1 > "$LOCAL_OUTPUT/word-count-results.txt"
        echo "词频文件已保存: $LOCAL_OUTPUT/word-count-results.txt"
    fi
    
    # 生成性能分析总结
    echo ""
    echo "=== 性能优化总结 ==="
    echo "1. 数据处理规模: 约311MB英文文本"
    echo "2. 任务并行度: Map任务自动分片, Reduce任务4个"
    echo "3. Combiner效果: 显著减少网络传输"
    echo "4. 内存优化: Map(1GB), Reduce(2GB)"
    echo "5. 总执行时间: ${EXECUTION_TIME} 秒"
    
    echo ""
    echo "题目三执行完成！"
    echo "HDFS结果路径: $OUTPUT_PATH"
    echo "本地结果路径: $LOCAL_OUTPUT"
    echo "性能报告: $LOCAL_OUTPUT/performance-report.txt"
    echo "词频结果: $LOCAL_OUTPUT/word-count-results.txt"
    
else
    echo "=== 作业执行失败 ==="
    echo "执行时间: ${EXECUTION_TIME} 秒"
    echo "请检查错误日志并重新运行"
    
    # 尝试获取作业日志
    echo "尝试获取最近的作业日志..."
    yarn application -list -appStates FINISHED,FAILED,KILLED | head -5
    
    exit 1
fi