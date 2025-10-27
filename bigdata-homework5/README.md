# 大数据作业5 - Hadoop WordCount 词频统计

## 项目概述

本项目使用Hadoop自带的MapReduce WordCount示例程序对股票评论数据进行词频统计。使用本地模式运行，无需启动HDFS和YARN服务。

## 数据文件

- **stock_data.csv**: 股票评论数据（约480KB，包含评论文本和情感标签）

## 快速开始

### 方法1: 在Docker容器中直接运行

```bash
# 确保容器在运行
docker ps | grep hadoop-homework5

# 如果容器未运行，启动它
docker start hadoop-homework5

# 进入容器并执行
docker exec -it hadoop-homework5 bash
cd /homework
bash run.sh
```

### 方法2: 从宿主机直接执行

```bash
docker exec hadoop-homework5 bash /homework/run.sh
```

## 运行结果

运行成功后会显示：
- **原始词频统计 (Top 30)**: 包含所有词汇
- **过滤停用词后的有意义词频统计 (Top 50)**: 排除介词、冠词、标点等无意义词汇
- **股票代码词频统计 (Top 20)**: 提取2-5个大写字母的股票代码
- **统计信息**: 总词数、有意义词数、过滤率

### 示例输出：

**原始统计:**
```
the     1794
to      1668
a       1280
AAP     765
...
```

**有意义词统计（已过滤停用词）:**
```
AAP     765   (苹果股票代码)
short   278   (做空)
like    267   (喜欢)
long    215   (做多)
volume  211   (交易量)
stock   158   (股票)
buy     142   (买入)
...
```

**股票代码统计:**
```
AAP     765   (Apple - 苹果)
BAC     174   (Bank of America - 美国银行)
GOOG    165   (Google - 谷歌)
AMZN    79    (Amazon - 亚马逊)
...
```

**统计信息:**
```
总唯一词数: 19,756
有意义词数: 17,661
过滤率: 10.6%
```

## 查看完整结果

```bash
# 查看前50个高频词
cat /homework/output/part-r-00000 | sort -t$'\t' -k2 -nr | head -50

# 查看前100个高频词
cat /homework/output/part-r-00000 | sort -t$'\t' -k2 -nr | head -100

# 查看所有结果
cat /homework/output/part-r-00000
```

在宿主机上查看：
```bash
cd "/mnt/d/南京大学/大三上/大数据/作业5"
cat output/part-r-00000 | sort -t$'\t' -k2 -nr | head -50
```

## 项目结构

```
作业5/
├── stock_data.csv               # 股票评论数据（输入）
├── output/                      # WordCount输出结果
│   └── part-r-00000            # 词频统计结果文件
├── run.sh                       # 一键运行脚本
├── README.md                    # 本文件
├── completion_checklist.md      # 完成检查清单
├── 作业5.pdf                    # 作业要求文档
└── .gitignore                   # Git忽略规则
```

## WordCount MapReduce 伪代码

### Map 阶段伪代码
```
function map(String key, String value):
    // key: 文件偏移量（忽略）
    // value: 一行文本内容
    
    // 将文本转换为小写
    text = value.toLowerCase()
    
    // 分词：按空格、标点等分隔符切分
    words = text.split("\\s+")
    
    // 输出每个单词和计数1
    for each word in words:
        if word is not empty:
            emit(word, 1)
```

### Reduce 阶段伪代码
```
function reduce(String key, Iterator<Integer> values):
    // key: 单词
    // values: 该单词对应的所有计数值（都是1）
    
    sum = 0
    
    // 累加所有计数值
    for each value in values:
        sum = sum + value
    
    // 输出单词和总计数
    emit(key, sum)
```

### Combiner 优化（可选）
```
function combine(String key, Iterator<Integer> values):
    // 在Map端进行局部聚合，减少网络传输
    // Combiner逻辑与Reducer相同
    
    sum = 0
    for each value in values:
        sum = sum + value
    
    emit(key, sum)
```

### 完整MapReduce流程
```
1. Input Split（输入分片）
   - 将stock_data.csv分成若干片段
   - 每个片段由一个Mapper处理

2. Map Phase（映射阶段）
   - Mapper读取文本行
   - 分词并输出<word, 1>键值对
   - 例：输入 "the stock is good"
         输出 <the, 1>, <stock, 1>, <is, 1>, <good, 1>

3. Shuffle & Sort（洗牌和排序）
   - 按key（单词）分组
   - 相同单词的所有value聚集在一起
   - 例：<the, [1, 1, 1, ...]>

4. Reduce Phase（规约阶段）
   - Reducer接收分组后的数据
   - 累加每个单词的计数
   - 例：<the, [1,1,1,...]> → <the, 1794>

5. Output（输出）
   - 写入输出文件 part-r-00000
   - 格式：单词<TAB>次数
```

## 技术说明

### 运行模式
本作业使用Hadoop的**本地模式（Standalone Mode）**运行：
- **优点**: 无需启动HDFS/YARN服务，配置简单，运行快速
- **原理**: 使用本地文件系统代替HDFS，单JVM进程运行MapReduce
- **适用场景**: 开发测试、小规模数据处理

### Hadoop WordCount说明
- **JAR位置**: `/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar`
- **类名**: `wordcount`
- **功能**: 统计文本中每个单词的出现次数
- **输出格式**: `单词<TAB>次数`

### 运行参数
```bash
-Dmapreduce.framework.name=local    # 使用本地模式
-Dfs.defaultFS=file:///            # 使用本地文件系统
```

## 常见问题

### 1. 容器未运行
```bash
docker ps -a | grep hadoop-homework5
docker start hadoop-homework5
```

### 2. 清理输出重新运行
```bash
rm -rf /homework/output /homework/input_temp
bash /homework/run.sh
```

### 3. 查看详细日志
在容器中直接运行命令可以看到完整的MapReduce执行日志

## 性能指标

运行 stock_data.csv (480KB) 的典型结果：
- **输入记录数**: ~500条
- **唯一词数**: ~19,756个
- **处理时间**: 约10-15秒
- **最高频词**: "the" (1794次)

## 作者
南京大学 - 大数据课程作业5

## 日期
2025年
