# 大数据实验4：Spark 编程

本项目使用 Spark RDD、Spark SQL 和 Spark MLlib 完成天池 O2O 优惠券使用预测赛题的数据分析与建模，目录已整理为按任务分区的输入输出结构，便于复现和查阅实验结果。

## 快速开始

### 数据准备
本项目依赖天池 O2O 优惠券预测赛题数据，请确保以下文件已放入 `data/raw/` 目录：
- `ccf_offline_stage1_train.csv` (Task 2, 3 必需)
- `ccf_offline_stage1_test_revised.csv` (Task 3 必需)
- `ccf_online_stage1_train.csv` (Task 1 必需)

### 运行方式

**方式一：Shell 脚本（推荐）**
```bash
# 进入项目
cd bigdata-project4

# 安装依赖
pip install -r requirements.txt

# 一键运行所有任务（自动处理依赖顺序）
bash run.sh
```

**方式二：分步运行**
```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 按顺序执行任务
python3 src/task1_rdd.py   # 必须最先运行
python3 src/task2_sql.py   # 依赖 Task 1 输出
python3 src/task3_mllib.py
```

**方式三：Docker 容器**
```bash
bash docker_run.sh
```

## 项目结构

```
bigdata-project4/
├── data/
│   ├── raw/                   # 原始数据（需手动放置）
│   │   ├── ccf_offline_stage1_train.csv
│   │   ├── ccf_offline_stage1_test_revised.csv
│   │   └── ccf_online_stage1_train.csv   # 请放置到此
│   └── processed/             # 预处理/特征工程中间结果（可选）
├── output/                    # 任务输出按 taskN 分类
│   ├── task1/
│   │   ├── coupon_counts/            # 任务1.1 优惠券使用次数
│   │   └── online_consumption_table/ # 任务1.2 商家优惠券使用表（文档要求名称）
│   ├── task2/
│   │   ├── coupon_time_dist/         # 任务2.1 优惠券时间分布
│   │   └── merchant_pos_ratio/       # 任务2.2 商家正样本比例
│   └── task3/
│       └── predictions/              # 任务3 预测结果
├── pictures/                  # 实验截图与可视化
├── src/
│   ├── task1_rdd.py           # RDD 统计
│   ├── task2_sql.py           # SQL 多维分析
│   └── task3_mllib.py         # MLlib 预测
├── Dockerfile
├── docker_run.sh
├── requirements.txt
└── run.sh
```

说明：各任务输出会落在各自的 `output/taskN/` 目录；首次运行前 `task2/`、`task3/` 为空，执行对应脚本后会自动写入结果，避免所有结果混在 `task1/`。

> 数据未随仓库提交，请将 `ccf_online_stage1_train.csv` 放入 `data/raw/`，否则任务一无法运行。

## 一、实验环境

* **推荐**: Docker 运行（镜像基于 `python:3.9-slim` + `openjdk-17-jre-headless`，`docker_run.sh` 一键执行）。
* **本机直跑**: 需已安装 **Java/JRE 11+**（设置 `JAVA_HOME`）、Python 3.9、`pyspark`、`numpy`。
* **系统**: Linux (Ubuntu/Debian) 验证通过；若使用 Mac/Windows，请确保 JDK 与 PySpark 安装完备。
* **Spark**: Apache Spark 3.x（随 PySpark 安装）。

## 二、实验思路

### 任务一：Spark RDD（基础统计）
* **目标**：按优惠券 ID 统计使用次数；按商家统计负样本/普通消费/正样本并排序。
* **核心思路**：用 RDD 的 `map`/`filter`/`reduceByKey` 完成分布式聚合。
* **实现要点**：
  * 一次 `map` 生成 `(Merchant_id, (neg, norm, pos))`，一次 `reduceByKey` 同时完成三类计数，避免多轮过滤与 Shuffle。
  * 结果落盘到 `output/task1/coupon_counts/` 与 `output/task1/online_consumption_table/`，供后续 SQL 任务直接复用。
* **注意**：输入需放在 `data/raw/ccf_online_stage1_train.csv`；运行前可清理旧输出以避免 Spark saveAsTextFile 报已存在。

### 任务二：Spark SQL（多维分析）
* **目标**：计算优惠券使用时间在上/中/下旬的分布；基于任务一的表计算商家正样本比例 Top10。
* **核心思路**：DataFrame + SQL API 完成透视与关联。
* **实现要点**：
  * 使用 `pivot` 将日期分段（<=10、11-20、>20）直接得到上/中/下旬概率；结果写入 `output/task2/coupon_time_dist/`。
  * 读取 `output/task1/online_consumption_table/` 文本，解析为 DataFrame，与原始 offline 数据分开存储；计算正样本比例并写入 `output/task2/merchant_pos_ratio/`。
* **注意**：运行顺序需先完成任务一再跑任务二，否则缺少 `online_consumption_table`。

### 任务三：Spark MLlib（预测建模）
* **目标**：预测用户领取优惠券后 15 天内是否核销（天池 O2O 二分类）。
* **模型**：逻辑回归（训练快、可解释、适合稀疏特征）。
* **特征工程**：
  * 折扣率 `discount_rate_val`（"150:20" → `1-20/150`）、满减门槛 `discount_man`、折扣类型 `discount_type` 捕捉促销强度与门槛。
  * 距离缺失填充 11（大于最大距离 10）保持序数意义，避免 -1 误导模型。
  * 时间特征：`day_of_week` + `salary_cycle`（1-5/15-20/25-31/其他）建模周末与发薪效应。
* **输出**：预测结果写入 `output/task3/predictions/`，`probability` 列已转字符串以便 CSV 保存；Top5 结果用于报告截图。

## 三、实验结果（含输出路径）

### 任务一结果（RDD）

输出目录：`output/task1/coupon_counts/`（优惠券使用次数）、`output/task1/online_consumption_table/`（商家三类计数）。

**1. 优惠券发放数量统计 (Top 10)**

![Task 1 Coupon Counts Top 10](pictures/task1_1_coupon_top10.png)

```text
fixed 11736
100044222 33
100206677 32
100103897 25
100194221 25
100138104 24
100190807 22
100103427 22
100055804 22
100084668 21
```
说明：“fixed” 表示限时低价活动，优惠券无独立 ID。

**2. 商家优惠券使用情况 (Top 10)** — 来源表名保持为 `online_consumption_table`

![Task 1 Merchant Stats](pictures/task1_2_merchant_stats.png)

格式: `<Merchant_id> <负样本> <普通消费> <正样本>`

```text
10001 118 792 11
10002 0 30 0
10003 0 110 0
10004 0 35 0
10005 0 26 0
10006 95 43 2
10007 0 495 0
10008 0 265 0
10009 0 109 0
10010 0 10 0
```

### 任务二结果（SQL）

输出目录：`output/task2/coupon_time_dist/`（上/中/下旬概率）、`output/task2/merchant_pos_ratio/`（正样本比例 Top10）。

**1. 优惠券使用时间分布统计 (Top 10)**

![Task 2 Coupon Time Distribution](pictures/task2_1_coupon_time_dist.png)

```text
+---------+--------------------+-------------------+------------------+
|Coupon_id|early_prob          |mid_prob           |late_prob         |
+---------+--------------------+-------------------+------------------+
|4937     |1.0                 |0.0                |0.0               |
|5925     |0.5                 |0.0                |0.5               |
|2088     |0.3333333333333333  |0.0                |0.6666666666666666|
|6194     |0.0                 |0.0                |1.0               |
|3959     |0.3157894736842105  |0.39473684210526316|0.2894736842105263|
|8433     |0.0                 |0.4                |0.6               |
|2162     |0.5                 |0.0                |0.5               |
|2069     |0.037037037037037035|0.0                |0.9629629629629629|
|9586     |0.0                 |0.14285714285714285|0.8571428571428571|
|467      |0.0                 |1.0                |0.0               |
+---------+--------------------+-------------------+------------------+
```

**2. 商家正样本比例统计 (Top 10)**

![Task 2 Merchant Positive Ratio](pictures/task2_2_merchant_pos_ratio.png)

```text
+-----------+-------------------+---+-----+
|Merchant_id|Pos_Ratio          |Pos|Total|
+-----------+-------------------+---+-----+
|56505      |0.3908450704225352 |222|568  |
|34115      |0.3870967741935484 |12 |31   |
|59705      |0.3825136612021858 |70 |183  |
|12512      |0.375              |21 |56   |
|43806      |0.3469387755102041 |17 |49   |
|49403      |0.33531409168081494|395|1178 |
|51005      |0.2916666666666667 |14 |48   |
|21008      |0.2846715328467153 |39 |137  |
|52413      |0.2727272727272727 |3  |11   |
|42110      |0.25925925925925924|7  |27   |
+-----------+-------------------+---+-----+
```

### 任务三结果（MLlib）

输出目录：`output/task3/predictions/`（`probability` 已转为字符串便于 CSV 保存）。

**逻辑回归预测结果 (Top 5)**

![Task 3 Prediction Results](pictures/task3_prediction_results.png)

```text
+-------+---------+-------------+--------------------+----------+
|User_id|Coupon_id|Date_received|         probability|prediction|
+-------+---------+-------------+--------------------+----------+
|4129537|     9983|     20160712|[0.89847071913976...|       0.0|
|6949378|     3429|     20160706|[0.96149738177192...|       0.0|
|2166529|     6928|     20160727|[0.99309251298069...|       0.0|
|2166529|     1808|     20160727|[0.97387168413210...|       0.0|
|6172162|     6500|     20160708|[0.90197254389440...|       0.0|
+-------+---------+-------------+--------------------+----------+
```

### 总结
完成 RDD 基础统计、SQL 透视分析与 MLlib 预测全流程；输出按任务分区存储，`online_consumption_table` 表名与实验要求保持一致，便于复现与查阅。


## 四、实验心得与体会

本次实验让我把 Spark 从“会跑示例代码”提升到“能围绕业务完成一条端到端流程”。在实现过程中，一方面体会到 RDD、SQL、MLlib 各自适用的场景：RDD 适合底层控制，SQL 适合复杂统计透视，MLlib 适合在已有特征基础上快速建模；另一方面也意识到环境配置和特征工程的重要性——用 Docker 统一运行环境能明显降低踩坑成本，而围绕业务含义设计折扣、距离和时间周期等特征，往往比堆叠更复杂的模型更关键。