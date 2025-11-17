#!/bin/bash

# Enhanced Performance Configuration Testing Script for Problem 3
# Tests different Map/Reduce task configurations and analyzes performance

echo "=== MapReduce Performance Configuration Testing ==="
echo "Date: $(date)"
echo "Testing multiple Map/Reduce configurations for performance analysis"
echo "==========================================================="

# Configuration
INPUT_PATH="/public/data/wordcount/all_books_merged.txt"
BASE_OUTPUT_PATH="/user/s522025320139/homework1/problem3"
JAR_FILE="hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar"
MAIN_CLASS="com.bigdata.assignment.problem3.WordCountConfigTestDriver"

# Clean up previous test results
echo ""
echo "### Cleaning up previous test results ###"
docker exec hadoop-client bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre && \
cd /home/hadoop/java_code/hadoop-assignment && \
hdfs dfs -rm -r -f ${BASE_OUTPUT_PATH}_config_tests
"

echo ""
echo "### Starting Multi-Configuration Performance Testing ###"
echo "This will test 6 different configurations:"
echo "1. 1 reducer, 64MB splits, with combiner"
echo "2. 2 reducers, 64MB splits, with combiner"  
echo "3. 4 reducers, 64MB splits, with combiner"
echo "4. 4 reducers, 128MB splits, with combiner"
echo "5. 4 reducers, 64MB splits, NO combiner"
echo "6. 8 reducers, 32MB splits, with combiner"
echo ""

# Run the comprehensive configuration test
docker exec hadoop-client bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre && \
cd /home/hadoop/java_code/hadoop-assignment && \
echo 'Starting comprehensive configuration testing...' && \
hadoop jar $JAR_FILE $MAIN_CLASS $INPUT_PATH ${BASE_OUTPUT_PATH}_config_tests
"

echo ""
echo "### Configuration Testing Completed ###"
echo "Results saved to: ${BASE_OUTPUT_PATH}_config_tests/"
echo ""
echo "### Collecting Performance Summary ###"

# Display summary of all test results
docker exec hadoop-client bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre && \
cd /home/hadoop/java_code/hadoop-assignment && \
echo '' && \
echo '=== Performance Summary Report ===' && \
echo 'Configuration | Processing Time | Throughput | Combiner Reduction' && \
echo '--------------------------------------------------------------------------' && \
for config in config1_1reducer_64mb_combiner config2_2reducer_64mb_combiner config3_4reducer_64mb_combiner config4_4reducer_128mb_combiner config5_4reducer_64mb_nocombiner config6_8reducer_32mb_combiner; do
    if hdfs dfs -test -e ${BASE_OUTPUT_PATH}_config_tests/\$config/performance-config-report.txt; then
        echo \"Processing \$config results...\"
        hdfs dfs -cat ${BASE_OUTPUT_PATH}_config_tests/\$config/performance-config-report.txt | grep -E '(total_processing_time|throughput_records_per_sec|combiner_reduction_rate)' | tr '\n' ' '
        echo \"\"
    fi
done
echo '==========================================================================='
"

echo ""
echo "### Individual Configuration Reports ###"
docker exec hadoop-client bash -c "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre && \
cd /home/hadoop/java_code/hadoop-assignment && \
echo 'Listing all performance reports:' && \
hdfs dfs -ls ${BASE_OUTPUT_PATH}_config_tests/*/performance-config-report.txt
"

echo ""
echo "### Performance Testing Instructions ###"
echo "To download all results locally, run:"
echo "docker exec hadoop-client hdfs dfs -get ${BASE_OUTPUT_PATH}_config_tests/ /home/hadoop/java_code/hadoop-assignment/"
echo ""
echo "To analyze individual configuration results:"
echo "docker exec hadoop-client hdfs dfs -cat ${BASE_OUTPUT_PATH}_config_tests/[config_name]/performance-config-report.txt"
echo ""
echo "Performance testing completed successfully!"
echo "==========================================================================="