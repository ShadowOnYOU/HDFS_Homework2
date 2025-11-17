# 作业完成情况检查清单

## 题目一：HDFS 操作与 WordCount 实现（30分）

### 功能要求检查

- [x] **HDFS 操作**（6分）
  - [x] 检查输入目录是否存在
  - [x] 删除已存在的输出目录
  - [x] 显示文件列表和统计信息
  
- [x] **MapReduce 实现**（18分）
  - [x] Mapper 实现：文本分词、词频统计
  - [x] Reducer 实现：词频聚合
  - [x] Driver 配置：作业参数配置
  
- [x] **输出格式**（需要补充）
  - [ ] ❌ **缺失 `words.txt`**：需要单独文件，格式 `单词\t词频`，按字典序排序
  - [ ] ❌ **缺失 `statistics.txt`**：需要包含以下统计项
    - `input_files\t[数值]`
    - `processing_time\t[数值]`
    - `total_words\t[数值]`
    - `unique_words\t[数值]`
  
- [x] **代码质量**（2分）
  - [x] 代码结构清晰
  - [x] 注释完整
  - [x] 命名规范

### 当前问题
1. **输出格式不符合要求**：需要生成 `words.txt` 和 `statistics.txt` 两个文件
2. **统计信息缺失**：需要从 Driver 中提取并保存统计信息

---

## 题目二：自定义 Combiner 和 Partitioner（40分）

### 功能要求检查

- [x] **Combiner 实现**（15分）
  - [x] WordCountCombiner 类实现
  - [x] 本地聚合逻辑
  - [x] 计数器统计（已实现）
  - [x] Driver 配置
  
- [x] **Partitioner 实现**（15分）
  - [x] AlphabetPartitioner 类实现
  - [x] 分区规则（A-F/G-N/O-S/T-Z）
  - [x] 边界情况处理
  - [x] 4个Reducer配置
  
- [ ] **输出格式**（6分，需要补充）
  - [ ] ❌ **缺失 `words.txt`**：需要单独文件，格式 `单词\t词频`，按字典序排序
  - [ ] ❌ **缺失 `statistics.txt`**：需要包含以下统计项
    - `combiner_input_records\t[数值]`
    - `combiner_output_records\t[数值]`
    - `partition_0_records\t[数值]`
    - `partition_1_records\t[数值]`
    - `partition_2_records\t[数值]`
    - `partition_3_records\t[数值]`
    - `total_words\t[数值]`
    - `unique_words\t[数值]`
  
- [?] **性能对比分析**（2分，可选）
  - [ ] 启用/禁用 Combiner 的对比数据
  
- [x] **代码质量**（2分）
  - [x] 代码结构清晰
  - [x] 注释完整

### 当前问题
1. **输出格式不符合要求**：需要生成独立的 `words.txt` 和 `statistics.txt`
2. **配置参数过于激进**：已修正为合理配置
3. **需要添加输出后处理**：合并多个分区的结果到单个 words.txt

---

## 题目三：MapReduce 任务调优与性能分析（30分）

### 功能要求检查

- [x] **HDFS 操作**
  - [x] 输入检查
  - [x] 结果展示
  
- [x] **MapReduce 实现**（12分）
  - [x] 基础 WordCount 功能
  - [x] 文本预处理
  
- [x] **任务调优配置**（10分）
  - [x] Map 任务控制
  - [x] Reduce 任务控制
  - [x] Combiner 优化
  
- [ ] **输出格式**（需要补充）
  - [ ] ❌ **缺失 `word-count-results.txt`**：需要按词频降序排列
  - [ ] ❌ **缺失 `performance-report.txt`**：需要包含以下统计项
    - `total_processing_time\t[数值]`
    - `map_tasks_count\t[数值]`
    - `reduce_tasks_count\t[数值]`
    - `input_records\t[数值]`
    - `output_records\t[数值]`
    - `total_words\t[数值]`
    - `combiner_enabled\t[true/false]`
    - `combiner_input_records\t[数值]`
    - `combiner_output_records\t[数值]`
  
- [x] **性能监控**（6分，部分完成）
  - [x] 记录执行时间
  - [x] 统计任务数量
  - [x] 监控 Combiner 效果
  - [ ] 需要保存到文件

### 当前问题
1. **输出格式不完整**：需要生成两个独立的结果文件
2. **结果排序要求**：word-count-results.txt 需要按词频降序排列
3. **性能报告需要独立文件**：performance-report.txt

---

## 总体待办事项（优先级排序）

### 高优先级（影响评分）

1. **题目一输出文件生成**
   - [ ] 实现 `words.txt` 生成（从 part-r-* 合并并排序）
   - [ ] 实现 `statistics.txt` 生成（从 Counters 提取）

2. **题目二输出文件生成**
   - [ ] 实现 `words.txt` 生成（合并4个分区文件）
   - [ ] 实现 `statistics.txt` 生成（包含分区统计）

3. **题目三输出文件生成**
   - [ ] 实现 `word-count-results.txt` 生成（按词频降序）
   - [ ] 实现 `performance-report.txt` 生成

### 中优先级（提升代码质量）

4. **添加结果后处理模块**
   - [ ] 创建 ResultProcessor 工具类
   - [ ] 实现文件合并和排序功能
   - [ ] 实现统计信息提取和保存

5. **运行脚本优化**
   - [ ] 更新运行脚本以生成所需输出文件
   - [ ] 添加结果验证步骤

### 低优先级（加分项）

6. **性能对比分析**（题目二）
   - [ ] 创建对比实验脚本
   - [ ] 生成性能对比报告

---

## 项目结构检查

- [x] `src/main/java/com/bigdata/assignment/problem1/` - 题目一代码
- [x] `src/main/java/com/bigdata/assignment/problem2/` - 题目二代码
- [x] `src/main/java/com/bigdata/assignment/problem3/` - 题目三代码
- [x] `scripts/` - 运行脚本
- [x] `pom.xml` - Maven 配置
- [x] `README.md` - 项目说明
- [ ] `output/problem1/` - 题目一输出（需要补充 words.txt 和 statistics.txt）
- [ ] `output/problem2/` - 题目二输出（需要补充 words.txt 和 statistics.txt）
- [ ] `output/problem3/` - 题目三输出（需要补充结果文件）
- [ ] `report/` - 实验报告目录（待完成）

---

## 评分影响分析

### 当前可能扣分项：

1. **题目一**（最多扣6分）
   - 输出格式不完整：-3分
   - 统计信息缺失：-3分

2. **题目二**（最多扣6分）
   - 输出格式不完整：-3分
   - 统计信息缺失：-3分

3. **题目三**（最多扣分未知）
   - 输出格式不符合要求
   - 性能报告缺失

**总计可能扣分：12-18分**

### 建议优先完成：
1. 立即实现输出文件生成功能（所有题目）
2. 确保统计信息完整（所有题目）
3. 验证结果格式符合要求

---

## 下一步行动计划

1. **创建结果后处理模块**（30分钟）
2. **修改三个 Driver 类添加结果导出**（1小时）
3. **测试所有输出文件生成**（30分钟）
4. **准备实验报告素材**（收集截图和数据）（1小时）

预计总耗时：3小时
