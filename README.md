# Hadoop MapReduce 作业 - WordCount 系列实验

## 项目概述

本项目实现了三个递进式的 MapReduce WordCount 程序，涵盖了 Hadoop MapReduce 框架的核心功能和性能优化技术。

### 题目说明

- **Problem 1**：基础 WordCount 实现，掌握 HDFS 操作与 MapReduce 基本编程
- **Problem 2**：使用 Combiner 和 Partitioner 优化，实现本地聚合和自定义数据分区
- **Problem 3**：MapReduce 任务调优与性能分析，探索参数优化和性能监控

## 环境要求

- **Hadoop 版本**：3.4.2
- **Java 版本**：1.8
- **Maven 版本**：3.6+
- **操作系统**：Linux（推荐）或 macOS

## 项目结构

```
HDFS_Homework2/
├── src/main/java/com/bigdata/assignment/
│   ├── problem1/          # 基础 WordCount
│   │   ├── WordCountMapper.java
│   │   ├── WordCountReducer.java
│   │   └── WordCountDriver.java
│   ├── problem2/          # Combiner + Partitioner
│   │   ├── WordCountMapper.java
│   │   ├── WordCountCombiner.java
│   │   ├── AlphabetPartitioner.java
│   │   ├── WordCountWithCombinerReducer.java
│   │   └── WordCountWithCombinerDriver.java
│   └── problem3/          # 性能优化版本
│       ├── WordCountOptimizedMapper.java
│       ├── WordCountOptimizedCombiner.java
│       ├── WordCountOptimizedReducer.java
│       └── WordCountOptimizedDriver.java
├── output/                # 程序输出结果
│   ├── problem1/
│   ├── problem2/
│   └── problem3/
├── report/                # 实验报告
│   ├── 实验报告.pdf
│   └── screenshots/
├── scripts/               # 运行脚本
├── pom.xml
└── README.md

```

## 编译说明

### 1. 使用 Maven 编译

```bash
mvn clean package -DskipTests
```

编译成功后，JAR 文件位于：`target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar`

### 2. 上传到 Hadoop 集群

```bash
scp target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar hadoop@<host>:~/hadoop-jars/
```

## 运行说明

### Problem 1：基础 WordCount

#### 功能说明
- 实现基本的单词计数功能
- 输出按字典序排序的单词统计结果
- 生成包含处理时间、单词数等统计信息的文件

#### 运行命令

```bash
hadoop jar ~/hadoop-jars/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem1.WordCountDriver \
  /user/<学号>/big-input \
  /user/<学号>/homework1/problem1
```

#### 输出文件
- `words.txt` - 按字典序排序的单词计数结果（格式：word\tcount）
- `statistics.txt` - 统计信息（input_files, processing_time, total_words, unique_words）

### Problem 2：Combiner + Partitioner 优化

#### 功能说明
- 使用 Combiner 进行本地聚合，减少网络传输
- 使用自定义 Partitioner 实现字母分区（A-F, G-N, O-S, T-Z）
- 4 个 Reducer 并行处理不同分区的数据

#### 运行命令

```bash
hadoop jar ~/hadoop-jars/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem2.WordCountWithCombinerDriver \
  /user/<学号>/big-input \
  /user/<学号>/homework1/problem2
```

#### 输出文件
- `words.txt` - 合并所有分区后的单词计数结果
- `statistics.txt` - 包含 Combiner 效率和分区统计的详细信息

#### 优化配置
- Combiner 最小 spill 数：1（确保 Combiner 被触发）
- Spill 阈值：80%
- 排序缓冲区：100MB

### Problem 3：性能优化与监控

#### 功能说明
- 开启 Map 输出压缩（Snappy）
- 优化缓冲区和 spill 参数
- 输出按**频率降序**排序的结果
- 生成详细的性能监控报告

#### 运行命令

```bash
hadoop jar ~/hadoop-jars/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem3.WordCountOptimizedDriver \
  /user/<学号>/big-input \
  /user/<学号>/homework1/problem3
```

#### 输出文件
- `word-count-results.txt` - 按频率降序排序的单词计数结果
- `performance-report.txt` - 详细的性能监控报告（包含 9 个关键指标）

#### 优化配置
- Split 大小：128MB（默认）
- 排序缓冲区：200MB
- Spill 阈值：80%
- Map 输出压缩：开启（Snappy Codec）
- Combiner 最小 spill 数：1

## 性能对比

| 指标 | Problem 1 | Problem 2 | Problem 3 |
|------|-----------|-----------|-----------|
| Map 任务数 | ~700 | ~700 | ~700 |
| Reduce 任务数 | 1 | 4 | 2 |
| Combiner | 无 | 有 | 有（优化） |
| Map 输出压缩 | 无 | 无 | 有（Snappy） |
| 预期执行时间 | 基准 | 更快 | 最快 |

## 关键技术点

### 1. HDFS 操作
- 文件系统初始化和路径检查
- 输出目录的自动清理
- 结果文件的合并和重命名

### 2. Combiner 优化
- 本地聚合减少网络传输
- 压缩率通常可达 90%+
- 需要注意 minspills 参数的设置

### 3. Partitioner 设计
- 基于首字母的字母分区
- 确保数据均衡分布到各 Reducer
- 支持非英文字符的默认分区

### 4. 性能调优
- 调整 split 大小影响 Map 任务数
- 优化排序缓冲区和 spill 阈值
- 启用 Map 输出压缩减少 I/O

## 常见问题

### Q1: 编译时出现依赖错误
**A**: 确保 Maven 配置正确，运行 `mvn clean install` 重新下载依赖。

### Q2: 运行时找不到主类
**A**: 检查 JAR 文件是否正确上传，使用完整的类名运行。

### Q3: Combiner 没有被触发
**A**: 检查 `mapreduce.map.combine.minspills` 参数，设置为 1 确保触发。

### Q4: 输出文件格式不正确
**A**: 确保使用最新版本的代码，已实现结果文件的合并和格式化功能。

## 作者信息

- **学号**：522025320139
- **课程**：大数据理论与实践
- **学期**：2025 研一上

## 许可证

本项目仅用于课程作业，未经许可不得用于其他用途。
