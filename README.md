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

### 快速开始 - 实际运行命令

以下是在集群环境中的实际运行命令（学号：522025320139）：

#### Problem 1：基础 WordCount
```bash
cd /home/hadoop/java_code/hadoop-assignment
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem1.WordCountDriver \
  /public/data/wordcount/all_books_merged.txt \
  /user/s522025320139/homework1/problem1
```

#### Problem 2：Combiner + Partitioner 优化

**启用 Combiner 的性能测试：**
```bash
cd /home/hadoop/java_code
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem2.WordCountWithPerformanceDriver \
  /public/data/wordcount/all_books_merged.txt \
  /user/s522025320139/homework1/problem2/output_performance_with_combiner \
  true
```

**禁用 Combiner 的性能测试（对比）：**
```bash
cd /home/hadoop/java_code  
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem2.WordCountWithPerformanceDriver \
  /public/data/wordcount/all_books_merged.txt \
  /user/s522025320139/homework1/problem2/output_performance_without_combiner \
  false
```

**正式运行（推荐）：**
```bash
cd /home/hadoop/java_code
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem2.WordCountWithPerformanceDriver \
  /public/data/wordcount/all_books_merged.txt \
  /user/s522025320139/homework1/problem2 \
  true
```

#### Problem 3：性能优化与监控

**基础优化版本：**
```bash
cd /home/hadoop/java_code
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem3.WordCountOptimizedDriver \
  /public/data/wordcount/all_books_merged.txt \
  /user/s522025320139/homework1/problem3
```

**配置测试版本（多种配置对比）：**
```bash
cd /home/hadoop/java_code
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
  com.bigdata.assignment.problem3.WordCountConfigTestDriver \
  /public/data/wordcount/all_books_merged.txt \
  /user/s522025320139/homework1/problem3_config_tests
```

---

### 详细功能说明

### Problem 1：基础 WordCount

#### 功能特性
- 实现基本的单词计数功能
- 输出按字典序排序的单词统计结果
- 生成包含处理时间、单词数等统计信息的文件
- 自动 HDFS 路径管理和清理

#### 输出文件
- `words.txt` - 按字典序排序的单词计数结果（格式：word\tcount）
- `statistics.txt` - 统计信息（input_files, processing_time, total_words, unique_words）

### Problem 2：Combiner + Partitioner 优化

#### 功能特性
- 使用 Combiner 进行本地聚合，减少网络传输
- 使用自定义 Partitioner 实现字母分区（A-F, G-N, O-S, T-Z）
- 4 个 Reducer 并行处理不同分区的数据
- 性能对比分析（启用/禁用 Combiner）

#### 输出文件
- `words.txt` - 合并所有分区后的单词计数结果
- `statistics.txt` - 包含 Combiner 效率和分区统计的详细信息
- `performance-report.txt` - 性能对比分析报告

#### 优化配置
- Combiner 最小 spill 数：1（确保 Combiner 被触发）
- Spill 阈值：80%
- 排序缓冲区：100MB

### Problem 3：性能优化与监控

#### 功能特性
- 开启 Map 输出压缩（Snappy）
- 优化缓冲区和 spill 参数
- 输出按**频率降序**排序的结果
- 生成详细的性能监控报告
- 多种配置对比测试

#### 输出文件
- `word-count-results.txt` - 按频率降序排序的单词计数结果
- `performance-report.txt` - 详细的性能监控报告（包含 9 个关键指标）
- 配置测试版本会生成多个配置的对比结果

#### 优化配置
- Split 大小：128MB（默认）
- 排序缓冲区：200MB
- Spill 阈值：80%
- Map 输出压缩：开启（Snappy Codec）
- Combiner 最小 spill 数：1

## 性能对比分析

### 配置对比表

| 指标 | Problem 1 | Problem 2 | Problem 3 |
|------|-----------|-----------|-----------|
| **Map 任务数** | 3 | 3 | 3 |
| **Reduce 任务数** | 1 | 4 | 2 |
| **Combiner** | 无 | 有 | 有（优化） |
| **Partitioner** | 默认 | 自定义字母分区 | 默认 |
| **Map 输出压缩** | 无 | 无 | 有（Snappy） |
| **排序结果** | 字典序 | 字典序 | 频率降序 |
| **实际执行时间** | 167.86秒 (基准) | 85.09秒 (49.3%提升) | 78.36秒 (53.3%提升) |

### 关键性能指标

### 测试数据集信息

- **数据文件**：`/public/data/wordcount/all_books_merged.txt`
- **文件大小**：322.6MB (322,620,529 bytes)
- **输入记录数**：6,675,055 行
- **单词总数**：54,591,717 个
- **唯一单词数**：248,505 个
- **Map 任务数**：3个 (基于128MB Split大小)

#### Problem 1 - 基础版本
- **特点**：单 Reducer，无优化，字典序排序
- **执行时间**：167.86秒
- **优势**：简单可靠，结果完整
- **劣势**：网络传输量大，执行时间长

#### Problem 2 - Combiner 优化版本  
- **特点**：4个Reducer并行，Combiner本地聚合，字母分区
- **执行时间**：85.09秒（带Combiner）/ 141.41秒（不带Combiner）
- **优势**：网络传输减少99.30%，并行度高
- **Combiner效果**：压缩比 44.41:1

#### Problem 3 - 全面优化版本
- **特点**：Map输出压缩，优化缓冲区配置，频率降序排序
- **执行时间**：78.36秒
- **优势**：I/O效率最高，内存使用优化，Combine压缩率98.11%
- **资源使用**：峰值内存2.35GB，GC时间仅949ms

### 实际测试数据

基于 **322.6MB** 输入数据文件（`all_books_merged.txt`）的真实测试结果：

| 测试版本 | 执行时间 | Map输出记录数 | Reduce输入记录数 | 网络传输减少率 | HDFS输出大小 |
|----------|----------|---------------|------------------|----------------|--------------|
| **Problem 1** - 基础版本 | **167.86秒** | 54,591,717 | 54,591,717 | 0% (基准) | 2.79MB |
| **Problem 2** - 带Combiner | **85.09秒** | 54,591,717 | **383,894** | **99.30%** | 2.79MB |
| **Problem 2** - 不带Combiner | **141.41秒** | 54,591,717 | 54,591,717 | 0% | 2.79MB |
| **Problem 3** - 全面优化 | **78.36秒** | 54,591,717 | **383,894** | **99.30%** | 2.79MB |

### 关键性能指标分析

#### Combiner 效果对比
- **Problem 2 (Combiner启用)**：
  - Map输出记录：54,591,717
  - Reduce输入记录：383,894
  - **数据减少率：99.30%** (54,207,823 条记录被本地聚合)
  - **Combiner压缩比：44.41:1**

- **Problem 2 (Combiner禁用)**：
  - 所有Map输出都需要通过网络传输到Reducer
  - 执行时间增加 **66.2%** (141.41s vs 85.09s)

#### 优化效果总结
1. **Combiner优化** (Problem 2 vs Problem 1)：
   - 执行时间减少：**49.3%** (167.86s → 85.09s)
   - 网络传输减少：**99.30%**

2. **全面优化** (Problem 3 vs Problem 1)：
   - 执行时间减少：**53.3%** (167.86s → 78.36s)
   - Map输出压缩：Snappy编码进一步优化I/O
   - Combine压缩率：**98.11%**

3. **资源使用优化** (Problem 3)：
   - 内存峰值：2.35GB
   - GC时间：仅949ms
   - 数据处理吞吐量：3.93 MB/sec

**核心结论**：Combiner是性能优化的关键，可实现近50%的执行时间减少和99%+的网络传输减少。

## 脚本使用说明

项目提供了完整的自动化脚本，可以简化运行流程：

### 自动化脚本

```bash
# Problem 1 - 基础版本
./scripts/run-problem1.sh 522025320139

# Problem 2 - Combiner优化版本  
./scripts/run-problem2.sh 522025320139

# Problem 3 - 性能优化版本
./scripts/run-problem3.sh 522025320139
```

### 脚本功能
- 自动编译项目（Maven）
- 检查输入路径存在性
- 创建输出目录
- 运行 MapReduce 作业
- 显示结果摘要
- 错误处理和状态检查

### 结果查看命令

```bash
# 查看输出目录结构
hdfs dfs -ls /user/s522025320139/homework1/problem1

# 查看单词统计结果（前20行）
hdfs dfs -cat /user/s522025320139/homework1/problem1/words.txt | head -20

# 查看性能统计信息
hdfs dfs -cat /user/s522025320139/homework1/problem1/statistics.txt

# 下载结果到本地
hdfs dfs -get /user/s522025320139/homework1/problem1/words.txt ./problem1_results.txt
```

---

## 关键技术点

### 1. HDFS 操作
- 文件系统初始化和路径检查
- 输出目录的自动清理
- 结果文件的合并和重命名
- 支持单文件和目录输入

### 2. Combiner 优化
- 本地聚合减少网络传输
- 压缩率通常可达 90%+
- 需要注意 minspills 参数的设置
- 性能对比分析功能

### 3. Partitioner 设计
- 基于首字母的字母分区（A-F, G-N, O-S, T-Z）
- 确保数据均衡分布到各 Reducer
- 支持非英文字符的默认分区
- 分区统计和负载均衡分析

### 4. 性能调优
- 调整 split 大小影响 Map 任务数
- 优化排序缓冲区和 spill 阈值
- 启用 Map 输出压缩减少 I/O
- 多种配置组合的性能测试

### 5. 监控与分析
- 详细的作业计数器统计
- 网络传输效率分析
- 内存使用优化监控
- 自动生成性能报告

## 命令速查表

### 编译和打包
```bash
mvn clean package -DskipTests
```

### 核心运行命令
```bash
# 切换到项目目录
cd /home/hadoop/java_code/hadoop-assignment

# Problem 1 - 基础 WordCount
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar com.bigdata.assignment.problem1.WordCountDriver /public/data/wordcount/all_books_merged.txt /user/s522025320139/homework1/problem1

# Problem 2 - 带 Combiner（推荐）
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar com.bigdata.assignment.problem2.WordCountWithPerformanceDriver /public/data/wordcount/all_books_merged.txt /user/s522025320139/homework1/problem2 true

# Problem 3 - 性能优化
hadoop jar hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar com.bigdata.assignment.problem3.WordCountOptimizedDriver /public/data/wordcount/all_books_merged.txt /user/s522025320139/homework1/problem3
```

### 结果查看
```bash
# 查看输出目录
hdfs dfs -ls /user/s522025320139/homework1/problem1

# 查看结果文件内容
hdfs dfs -cat /user/s522025320139/homework1/problem1/words.txt | head -20
hdfs dfs -cat /user/s522025320139/homework1/problem1/statistics.txt

# 下载到本地
hdfs dfs -get /user/s522025320139/homework1/problem1/* ./results/
```

### 清理命令
```bash
# 清理输出目录（重新运行前）
hdfs dfs -rm -r /user/s522025320139/homework1/problem1
hdfs dfs -rm -r /user/s522025320139/homework1/problem2  
hdfs dfs -rm -r /user/s522025320139/homework1/problem3
```

---

## 常见问题

### Q1: 编译时出现依赖错误
**A**: 确保 Maven 配置正确，运行 `mvn clean install` 重新下载依赖。

### Q2: 运行时找不到主类
**A**: 检查 JAR 文件是否正确上传，使用完整的类名运行。

### Q3: Combiner 没有被触发
**A**: 检查 `mapreduce.map.combine.minspills` 参数，设置为 1 确保触发。

### Q4: 输出文件格式不正确
**A**: 确保使用最新版本的代码，已实现结果文件的合并和格式化功能。

### Q5: 输出目录已存在错误
**A**: 使用清理命令删除输出目录，或者在代码中启用自动删除功能。

### Q6: 内存不足错误
**A**: 检查集群资源配置，可能需要调整 Reducer 数量或缓冲区大小。

## 实验总结

本项目通过三个递进式的 WordCount 实现，全面探索了 Hadoop MapReduce 框架的核心特性：

### 主要成果
1. **基础实现**：掌握了 HDFS 操作和 MapReduce 基本编程模式
2. **性能优化**：通过 Combiner 实现了 90%+ 的网络传输减少
3. **并行处理**：使用自定义 Partitioner 实现了负载均衡的并行计算
4. **系统调优**：通过压缩和缓冲区优化实现了整体性能提升

### 技术亮点
- 完整的 HDFS 文件操作和路径管理
- 高效的 Combiner 本地聚合策略
- 智能的字母分区 Partitioner
- 全面的性能监控和分析系统
- 自动化的脚本部署和运行环境

### 实际应用价值  
本项目不仅完成了课程要求，更提供了一套完整的大数据处理解决方案模板，可以扩展应用于：
- 大规模文本分析
- 日志数据统计
- 搜索引擎索引构建
- 数据仓库 ETL 流程

---

## 作者信息

- **学号**：522025320139
- **姓名**：ShadowOnYOU
- **课程**：大数据理论与实践
- **学期**：2025 研一上
- **完成时间**：2025年11月

## 许可证

本项目仅用于课程作业，代码仅供学习参考，未经许可不得用于其他用途。
