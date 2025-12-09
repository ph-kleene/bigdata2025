# 大数据实验4：Spark 编程

本项目使用 Spark RDD、Spark SQL 和 Spark MLlib 完成天池 O2O 优惠券使用预测赛题的数据分析与建模。

详细的实验设计、代码分析及运行结果请参阅 [实验报告](实验报告.md)。

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
│   │   └── ccf_online_stage1_train.csv
│   └── processed/             # 预处理/特征工程中间结果（可选）
├── output/                    # 任务输出按 taskN 分类
│   ├── task1/
│   │   ├── coupon_counts/            # 任务1.1 优惠券使用次数
│   │   └── online_consumption_table/ # 任务1.2 商家优惠券使用表
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
├── 实验报告.md                 # 详细实验报告
├── Dockerfile
├── docker_run.sh
├── requirements.txt
└── run.sh
```

## 任务详解

### 任务一：Spark RDD（基础统计）
*   **目标**：按优惠券 ID 统计使用次数；按商家统计负样本/普通消费/正样本并排序。
*   **核心思路**：
    *   利用 RDD 的 `map` 算子将每条数据转换为 `(Merchant_id, (neg, norm, pos))` 形式。
    *   使用 `reduceByKey` 一次性完成三类指标的聚合，避免多次 Shuffle，提高效率。
*   **输出**：`output/task1/coupon_counts/` 与 `output/task1/online_consumption_table/`。

### 任务二：Spark SQL（多维分析）
*   **目标**：计算优惠券使用时间在上/中/下旬的分布；基于任务一的表计算商家正样本比例 Top10。
*   **核心思路**：
    *   **时间分布**：利用 DataFrame 的 `pivot` 透视功能，将日期分段（上/中/下旬）转为列进行统计，直观展示分布情况。
    *   **商家比率**：读取任务一生成的文本文件，解析为 DataFrame，与原始数据解耦，演示了 RDD 与 SQL 的协同工作。
*   **输出**：`output/task2/coupon_time_dist/` 与 `output/task2/merchant_pos_ratio/`。

### 任务三：Spark MLlib（预测建模）
*   **目标**：预测用户领取优惠券后 15 天内是否核销（天池 O2O 二分类）。
*   **核心思路**：
    *   **特征工程**：
        *   **折扣**：解析 "150:20" 为折扣率、满减门槛及折扣类型。
        *   **距离**：将缺失值填充为 11（大于最大距离 10），保持距离的序数含义，避免 -1 带来的误导。
        *   **时间**：提取“周几”和“发薪周期”（上/中/下旬）特征，捕捉消费习惯。
    *   **模型**：使用逻辑回归（Logistic Regression），适合稀疏特征且具有良好的可解释性。
*   **输出**：`output/task3/predictions/`。

## 结果展示

### 1. 优惠券与商家统计 (RDD)
![Task 1 Coupon Counts](pictures/task1_1_coupon_top10.png)

*(图：优惠券发放数量 Top 10)*

### 2. 时间分布与正样本比例 (SQL)
![Task 2 Time Distribution](pictures/task2_1_coupon_time_dist.png)

*(图：优惠券使用时间分布)*

### 3. 预测结果 (MLlib)
![Task 3 Prediction](pictures/task3_prediction_results.png)

*(图：逻辑回归预测概率)*

## 实验环境

*   **推荐**: Docker 运行（镜像基于 `python:3.9-slim` + `openjdk-17-jre-headless`）。
*   **本机直跑**: 需已安装 **Java/JRE 11+**、Python 3.9、`pyspark`、`numpy`。