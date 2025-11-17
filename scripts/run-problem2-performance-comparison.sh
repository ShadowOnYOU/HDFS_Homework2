#!/bin/bash

# Performance Comparison Script for Problem 2
# Compares WordCount performance with and without Combiner

echo "=== Problem 2 Performance Comparison Analysis ==="
echo "Date: $(date)"
echo "Testing Combiner vs No-Combiner performance"
echo "================================================"

# Configuration
INPUT_PATH="/public/data/wordcount/all_books_merged.txt"
BASE_OUTPUT_PATH="/user/s522025320139/homework1/problem2"
JAR_FILE="hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar"
MAIN_CLASS="com.bigdata.assignment.problem2.WordCountWithPerformanceDriver"

# Test 1: With Combiner (Default)
echo ""
echo "### Test 1: Running WordCount WITH Combiner ###"
OUTPUT_WITH_COMBINER="${BASE_OUTPUT_PATH}_with_combiner_test"

docker exec hadoop-client bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre && \
cd /home/hadoop/java_code/hadoop-assignment && \
echo 'Starting Test 1: WITH Combiner...' && \
hadoop jar $JAR_FILE $MAIN_CLASS $INPUT_PATH $OUTPUT_WITH_COMBINER true
"

echo ""
echo "Test 1 completed. Waiting 10 seconds before next test..."
sleep 10

# Test 2: Without Combiner
echo ""
echo "### Test 2: Running WordCount WITHOUT Combiner ###" 
OUTPUT_WITHOUT_COMBINER="${BASE_OUTPUT_PATH}_no_combiner_test"

docker exec hadoop-client bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre && \
cd /home/hadoop/java_code/hadoop-assignment && \
echo 'Starting Test 2: WITHOUT Combiner...' && \
hadoop jar $JAR_FILE $MAIN_CLASS $INPUT_PATH $OUTPUT_WITHOUT_COMBINER false
"

echo ""
echo "### Performance Comparison Completed ###"
echo "Results saved to:"
echo "  With Combiner: $OUTPUT_WITH_COMBINER"
echo "  Without Combiner: $OUTPUT_WITHOUT_COMBINER"
echo ""
echo "To view detailed comparison, check the job outputs above."
echo "Key metrics to compare:"
echo "- Total processing time"
echo "- Map output records vs Reduce input records"
echo "- Data reduction percentage"
echo "- Network shuffle data volume"
echo "================================================"