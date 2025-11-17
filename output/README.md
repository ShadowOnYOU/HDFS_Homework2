# HDFS结果文件下载说明

**下载时间**：2025年11月16日  
**下载路径**：从HDFS下载到本地 `output/` 目录

---

## 已下载的文件清单

### Problem 1 - 基础 WordCount
```
output/problem1/
├── statistics.txt        # 统计信息（167.86秒）
└── words.txt            # 完整词频结果（248,505个单词，2.79MB）
```

**statistics.txt 内容**：
```
input_files      1
processing_time  167860 (毫秒)
total_words      54591717
unique_words     248505
```

### Problem 2 - Combiner + Partitioner
```
output/problem2/
├── statistics.txt        # 统计信息（99.78秒，提升40.6%）
└── words.txt            # 完整词频结果（248,505个单词，2.79MB）
```

**statistics.txt 包含额外信息**：
- Combiner效果统计
- 分区分布信息
- 压缩率数据

### Problem 3 - 全面优化
```
output/problem3/
├── performance-report.txt      # 性能监控报告（78.36秒）
└── word-count-results.txt      # 按频率排序的词频（2.79MB）
```

**performance-report.txt 内容**：
```
total_processing_time       78356 (毫秒)
input_files                 1
input_size_bytes            322620529
map_tasks_count             700
reduce_tasks_count          2
total_words                 54591717
unique_words                248505
combiner_enabled            true
combiner_compression_ratio  98.11
```

---

## 下载命令记录

### 1. 设置环境变量
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre
```

### 2. 从HDFS下载文件到容器
```bash
# Problem 1
hdfs dfs -get /user/s522025320139/homework1/problem1_bigtest/statistics.txt .
hdfs dfs -get /user/s522025320139/homework1/problem1_bigtest/words.txt .

# Problem 2
hdfs dfs -get /user/s522025320139/homework1/problem2_bigtest/statistics.txt .
hdfs dfs -get /user/s522025320139/homework1/problem2_bigtest/words.txt .

# Problem 3
hdfs dfs -get /user/s522025320139/homework1/problem3_bigtest/performance-report.txt .
hdfs dfs -get /user/s522025320139/homework1/problem3_bigtest/word-count-results.txt .
```

### 3. 从容器复制到本地
```bash
docker cp hadoop-client:/home/hadoop/statistics.txt ./output/problem1/
docker cp hadoop-client:/home/hadoop/words.txt ./output/problem1/
# ... (依此类推)
```

---

## 文件验证

### 文件大小验证
```bash
ls -lh output/problem1/
ls -lh output/problem2/
ls -lh output/problem3/
```

**预期结果**：
- statistics.txt / performance-report.txt: ~2KB
- words.txt / word-count-results.txt: ~2.79MB (248,505行)

### 内容验证
```bash
# 验证统计信息
head -5 output/problem1/statistics.txt

# 验证词频结果
head -10 output/problem1/words.txt
wc -l output/problem1/words.txt  # 应该是 248505 或 248506 行
```

---

## 文件说明

### statistics.txt (Problem 1 & 2)
- **格式**：`key\tvalue`（制表符分隔）
- **用途**：记录作业执行的统计信息
- **关键指标**：
  - processing_time: 执行时间（毫秒）
  - total_words: 总单词数
  - unique_words: 唯一单词数

### words.txt (Problem 1 & 2)
- **格式**：`word\tcount`（制表符分隔）
- **排序**：按单词字典序排序
- **内容**：所有248,505个唯一单词及其出现次数

### performance-report.txt (Problem 3)
- **格式**：`metric\tvalue`（制表符分隔）
- **用途**：详细的性能监控报告
- **包含指标**：
  - 执行时间
  - 任务数量
  - Combiner压缩率
  - 数据处理量

### word-count-results.txt (Problem 3)
- **格式**：`word\tcount`（制表符分隔）
- **排序**：按词频降序排序（高频词在前）
- **特点**：便于查看最常见的单词

---

## 文件用途说明

这些文件用于：
1. **实验报告验证**：证明程序成功运行并产生正确结果
2. **性能分析**：对比三个版本的执行效率
3. **结果展示**：在报告中展示词频统计结果
4. **提交材料**：作为实验完整性的证明

---

## 注意事项

1. **文件完整性**：所有文件都已成功下载，无损坏
2. **数据一致性**：三个Problem的unique_words都是248,505，验证了结果的一致性
3. **文件编码**：所有文件为UTF-8编码，无乱码
4. **路径说明**：output/目录包含在.gitignore中，不会被提交到Git

---

## 快速查看命令

```bash
# 查看Problem 1统计信息
cat output/problem1/statistics.txt

# 查看Problem 1前20个单词
head -20 output/problem1/words.txt

# 查看Problem 3性能报告
cat output/problem3/performance-report.txt

# 查看Problem 3高频词Top 20
head -20 output/problem3/word-count-results.txt
```

---

**下载完成时间**：2025年11月16日 05:59
**下载状态**：✅ 全部成功
**文件总数**：6个文件
**文件总大小**：约8.4MB
