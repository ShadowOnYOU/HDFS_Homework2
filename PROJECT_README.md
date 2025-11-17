# Hadoop MapReduce Assignment

## 项目概述

本项目是《大数据理论与实践》课程的**实践作业二**，通过三个递进式的编程题目，系统掌握 Hadoop 生态系统中的核心技术：**HDFS 基础操作**、**MapReduce 编程模型**，以及 **MapReduce 高级特性**和**复杂数据处理技术**。

## 项目结构

```
hadoop-mapreduce-assignment/
├── src/                                    # 源代码目录
│   └── main/
│       ├── java/                          # Java 源文件
│       │   └── com/bigdata/assignment/
│       │       ├── problem1/              # 题目一：HDFS 操作与 WordCount 实现
│       │       │   ├── WordCountMapper.java       # Mapper 类
│       │       │   ├── WordCountReducer.java      # Reducer 类
│       │       │   └── WordCountDriver.java       # Driver 主程序
│       │       ├── problem2/              # 题目二：自定义 Combiner 和 Partitioner
│       │       │   ├── WordCountMapper.java           # Mapper 类
│       │       │   ├── WordCountCombiner.java         # Combiner 类
│       │       │   ├── AlphabetPartitioner.java       # Partitioner 类
│       │       │   ├── WordCountWithCombinerReducer.java # Reducer 类
│       │       │   └── WordCountWithCombinerDriver.java  # Driver 主程序
│       │       └── problem3/              # 题目三：MapReduce 任务调优与性能分析
│       │           ├── WordCountOptimizedMapper.java    # 优化版 Mapper 类
│       │           ├── WordCountOptimizedReducer.java   # 优化版 Reducer 类
│       │           ├── WordCountOptimizedCombiner.java  # 优化版 Combiner 类
│       │           └── WordCountOptimizedDriver.java    # 优化版 Driver 主程序
│       └── resources/                     # 配置文件目录
│           └── log4j.properties           # 日志配置
├── output/                                # 程序输出结果
│   ├── problem1/                         # 题目一输出
│   ├── problem2/                         # 题目二输出
│   └── problem3/                         # 题目三输出
├── scripts/                              # 运行脚本
│   ├── run-problem1.sh                   # 题目一运行脚本
│   ├── run-problem2.sh                   # 题目二运行脚本
│   └── run-problem3.sh                   # 题目三运行脚本
├── report/                               # 实验报告目录
│   └── screenshots/                      # 截图目录
│       ├── problem1/                     # 题目一相关截图
│       ├── problem2/                     # 题目二相关截图
│       └── problem3/                     # 题目三相关截图
├── pom.xml                               # Maven 构建文件
└── PROJECT_README.md                     # 项目说明文档
```

## 环境要求

### 开发环境
- **Java 版本**: JDK 8 (必须)
- **Hadoop 版本**: 3.4.2
- **构建工具**: Maven 3.6+
- **开发工具**: 推荐 IntelliJ IDEA 或 Eclipse

### 运行环境
- **集群类型**: 共享 Hadoop 3.4.2 集群
- **集群组件**: HDFS + YARN + MapReduce
- **访问方式**: 通过配置文件连接远程集群

## 快速开始

### 1. 环境配置

确保已配置好Hadoop环境变量：
```bash
export JAVA_HOME=/usr/local/opt/openjdk@8
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

### 2. 编译项目

```bash
cd hadoop-mapreduce-assignment
mvn clean package
```

### 3. 运行题目

#### 题目一：HDFS操作与WordCount实现
```bash
./scripts/run-problem1.sh <学号>
# 示例: ./scripts/run-problem1.sh 2021001001
```

#### 题目二：自定义Combiner和Partitioner
```bash
./scripts/run-problem2.sh <学号>
# 示例: ./scripts/run-problem2.sh 2021001001
```

#### 题目三：MapReduce任务调优与性能分析
```bash
./scripts/run-problem3.sh <学号>
# 示例: ./scripts/run-problem3.sh 2021001001
```

## 题目详述

### 题目一：HDFS操作与WordCount实现 (30分)

**主要功能**：
- 实现标准的WordCount程序
- 进行HDFS文件操作
- 生成基础的统计信息

**输出文件**：
- `words.txt`: 词频统计结果
- `statistics.txt`: 处理统计信息

**关键特性**：
- 文本预处理和清理
- HDFS目录检查和管理
- 基础的MapReduce编程模型

### 题目二：自定义Combiner和Partitioner (40分)

**主要功能**：
- 实现Combiner进行Map端本地聚合
- 实现按字母分区的Partitioner
- 分析Combiner的性能优化效果

**分区策略**：
- 分区0: A-F开头的单词
- 分区1: G-N开头的单词  
- 分区2: O-S开头的单词
- 分区3: T-Z开头的单词

**输出文件**：
- `words.txt`: 词频统计结果
- `statistics.txt`: Combiner和分区统计信息

**关键特性**：
- Combiner数据压缩分析
- 分区负载均衡验证
- 网络传输优化效果

### 题目三：MapReduce任务调优与性能分析 (30分)

**主要功能**：
- 大规模数据处理性能监控
- Map/Reduce任务数量调优
- 详细的性能分析报告

**优化特性**：
- Map任务并行度控制
- 内存配置优化
- Combiner效果分析
- 数据吞吐量计算

**输出文件**：
- `word-count-results.txt`: 按频次排序的词频结果
- `performance-report.txt`: 详细性能报告

## 测试数据

项目使用HDFS公共数据目录 `/public/data/wordcount/` 中的测试文件：

1. **简单测试文件** (`simple-test.txt`): 414字节，适合功能验证
2. **中等规模文件** (`alice-in-wonderland.txt`): 148KB，《爱丽丝梦游仙境》
3. **大规模文件** (`pride-and-prejudice.txt`): 735KB，《傲慢与偏见》
4. **超大规模文件** (`all_books_merged.txt`): 311MB，614本经典文学作品合集

## 编译说明

### 使用Maven编译
```bash
# 清理并编译
mvn clean compile

# 打包JAR文件
mvn clean package

# 跳过测试打包
mvn clean package -DskipTests
```

### 手动编译（可选）
```bash
# 编译Java源码
javac -cp $(hadoop classpath) -d target/classes src/main/java/com/bigdata/assignment/*/*.java

# 打包JAR文件
jar cf hadoop-mapreduce-assignment.jar -C target/classes .
```

## 运行说明

### 直接运行MapReduce程序

```bash
# 题目一
hadoop jar target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
    com.bigdata.assignment.problem1.WordCountDriver \
    /public/data/wordcount /<学号>/homework1/problem1

# 题目二  
hadoop jar target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
    com.bigdata.assignment.problem2.WordCountWithCombinerDriver \
    /public/data/wordcount /<学号>/homework1/problem2

# 题目三
hadoop jar target/hadoop-mapreduce-assignment-1.0-SNAPSHOT.jar \
    com.bigdata.assignment.problem3.WordCountOptimizedDriver \
    /public/data/wordcount /<学号>/homework1/problem3
```

### 查看结果

```bash
# 查看输出目录
hdfs dfs -ls /<学号>/homework1/problem1

# 查看词频统计结果
hdfs dfs -cat /<学号>/homework1/problem1/part-r-00000

# 查看统计信息
hdfs dfs -cat /<学号>/homework1/problem1/statistics.txt

# 下载结果到本地
hdfs dfs -get /<学号>/homework1/problem1/* ./output/problem1/
```

## 输出格式说明

### 词频统计文件格式
```
单词1    频次1
单词2    频次2
...
```

### 统计信息文件格式
```
统计项名称1    数值1  
统计项名称2    数值2
...
```

## 性能优化要点

### 题目二优化
- **Combiner**: 在Map端进行本地聚合，减少网络传输
- **Partitioner**: 按字母分区，实现负载均衡
- **监控指标**: Combiner压缩比、分区数据分布

### 题目三优化  
- **任务并行度**: 通过分片大小控制Map任务数
- **内存配置**: 优化Map/Reduce任务内存分配
- **性能监控**: 详细的执行时间和吞吐量分析

## 常见问题

### 编译问题
1. **Java版本**: 确保使用JDK 8
2. **Hadoop路径**: 检查HADOOP_HOME环境变量
3. **依赖冲突**: 使用Maven管理依赖

### 运行问题
1. **输出目录存在**: 自动删除已存在的输出目录
2. **权限问题**: 确保对HDFS目录有读写权限
3. **内存不足**: 调整mapreduce.map.memory.mb配置

### HDFS问题
1. **连接失败**: 检查集群配置文件
2. **文件不存在**: 确认公共数据目录路径
3. **空间不足**: 清理不需要的输出目录

## 实验报告要求

实验报告应包含以下内容：

1. **算法设计思路**: 每个题目的核心实现逻辑
2. **运行结果展示**: 包含完整的运行截图和日志
3. **性能分析**: Combiner效果分析、分区负载分析、调优效果对比
4. **问题与解决**: 实现过程中遇到的技术难点和解决方案
5. **实验收获**: 对MapReduce编程模型和性能优化的理解

## 提交要求

### 提交内容
- 完整的源代码项目
- 编译后的JAR文件  
- 运行结果文件
- 详细的实验报告
- 运行截图和日志

### 文件命名
- 压缩包: `学号-姓名-hadoop-assignment.zip`
- 报告文件: `学号-姓名-实验报告.pdf`

## 联系方式

如遇到技术问题，请通过以下方式寻求帮助：
- 课程微信群讨论
- 查阅Hadoop官方文档
- 参考MapReduce编程指南

## 参考资料

- [Hadoop官方文档](https://hadoop.apache.org/docs/stable/)
- [MapReduce教程](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [HDFS用户指南](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)