# 金融大数据技术实践：HDFS、MapReduce、Hive 与 Spark

本仓库归档 2025 年「金融大数据技术」课程的四个实验模块，包含源代码、运行脚本、实验报告、统计结果和截图。

优惠券分析模块使用天池 O2O 数据，围绕商家消费类型、顾客距离、领券到核销的时间、折扣与用户活跃度开展统计分析，并使用 Spark MLlib 构建「领取后 15 天内是否核销」的逻辑回归示例。Hadoop 基础模块使用股票评论文本，演示 HDFS 文件操作、YARN 作业运行与 WordCount 词频统计。

## 模块导航

| 模块 | 分析内容 | 代码与说明 | 实验记录 |
| --- | --- | --- | --- |
| Hadoop / HDFS / YARN | 股票评论 WordCount；数据上传、作业执行与 Web UI 监控 | [模块说明](bigdata-homework5/README.md)、[服务启动](bigdata-homework5/start_hadoop.sh)、[WordCount 脚本](bigdata-homework5/run_cluster.sh) | [实验报告与计数器](bigdata-homework5/README.md)、[运行截图](bigdata-homework5/picture/) |
| MapReduce | 商家消费分类、去重顾客距离、平均核销间隔、折扣与用户活跃度分析 | [模块说明](bigdata-project2/README.md)、[Mapper / Reducer](bigdata-project2/src/) | [实验报告](bigdata-project2/实验报告.md)、[结果汇总](bigdata-project2/RESULTS_SUMMARY.md)、[图表](bigdata-project2/output/) |
| Hive | 数据建表与导入、线上行为统计、距离与正样本比例、时间与折扣分析 | [模块说明](bigdata-project3/README.md)、[HiveQL 脚本](bigdata-project3/scripts/)、[容器配置](bigdata-project3/docker-compose.yml) | [实验报告与问题记录](bigdata-project3/实验报告.md)、[查询截图](bigdata-project3/pictures/) |
| Spark | RDD 聚合、SQL 透视与排序、MLlib 二分类预测 | [模块说明](bigdata-project4/README.md)、[Python 源码](bigdata-project4/src/) | [实验报告](bigdata-project4/实验报告.md)、[统计与预测输出](bigdata-project4/output/)、[运行截图](bigdata-project4/pictures/) |

## 实现要点

- **MapReduce 两阶段分析**：先按用户聚合领券数与使用数，再按活跃度分组统计；距离分析使用集合进行用户去重。[任务四源码](bigdata-project2/src/task4/)、[距离 Reducer](bigdata-project2/src/task2/reducer.py)
- **Hive 结构化查询**：将线上、线下 CSV 导入表，使用条件聚合区分未核销、普通消费与核销样本，并计算商家、距离和时间维度的统计结果。[基础查询](bigdata-project3/scripts/task2_basic_query.sql)、[聚合分析](bigdata-project3/scripts/task3_aggregation.sql)、[复杂查询](bigdata-project3/scripts/task4_complex_query.sql)
- **Spark RDD 与 SQL 衔接**：RDD 生成商家消费分类表，SQL 读取该输出计算正样本比例；另用 `pivot` 统计优惠券在上、中、下旬的使用分布。[RDD 实现](bigdata-project4/src/task1_rdd.py)、[SQL 实现](bigdata-project4/src/task2_sql.py)
- **核销预测特征工程**：提取折扣率、满减门槛、折扣类型、距离、领券星期和日期周期六个特征，经 `VectorAssembler` 输入逻辑回归，输出核销概率。[MLlib 实现](bigdata-project4/src/task3_mllib.py)

## 结果与数据口径

仓库保留了历史实验的报告、图表及部分输出文件。以下规模来自原有实验文档，原始数据未随仓库提供；复现时应以实际输入文件及查询计数为准。

| 模块 | 原有文档记载 | 来源 |
| --- | --- | --- |
| Hadoop WordCount | 6,090 条输入记录，输入文件约 472 KB | [实验报告](bigdata-homework5/README.md) |
| MapReduce | 75 万余条记录、约 43 MB；结果涉及 8,018 个商家、5,462 个优惠券 | [模块说明](bigdata-project2/README.md)、[结果汇总](bigdata-project2/RESULTS_SUMMARY.md) |
| Hive | 线下数据 1,048,575 条；另导入线上数据 | [实验报告](bigdata-project3/实验报告.md) |
| Spark | 使用线上、线下训练数据及线下测试数据；保留统计结果与预测文件 | [数据准备说明](bigdata-project4/README.md)、[任务输出](bigdata-project4/output/) |

可直接查看的成果包括 [MapReduce 商家消费分类图](bigdata-project2/output/task1_visualization.png)、[核销间隔图](bigdata-project2/output/task3_visualization.png)、[Hive 数据计数截图](bigdata-project3/pictures/task1_offline_count.png)、[Spark 时间分布截图](bigdata-project4/pictures/task2_1_coupon_time_dist.png)和 [MLlib 预测截图](bigdata-project4/pictures/task3_prediction_results.png)。

### 运行模式与模型现状

- Hadoop WordCount 的实验报告记录的是单机伪分布式 HDFS / YARN；目录内同时保留[本地模式脚本](bigdata-homework5/run.sh)。
- MapReduce 的优惠券实验采用 Hadoop 本地执行模式；Hive SQL 也配置了本地执行模式。Docker 多容器配置用于搭建实验服务，现有材料未提供多机集群的性能对比。
- 三个 Spark 任务均使用 `local[*]`。MLlib 示例在训练数据上拟合后对测试数据预测，当前代码没有划分验证集，也没有计算 AUC 等模型评估指标。
- 图表展示的是样本内的描述性统计；预测文件和运行截图记录的是流程输出，模型效果仍需独立验证。

## 运行与复现

各模块环境独立，建议在 Linux 或 WSL 的 Bash 环境中按需运行。Hadoop 与 Hive 需要对应的服务环境；Spark 本机运行需要 Python、Java 和 PySpark。完整部署步骤见各模块说明。

### 1. 准备数据

| 模块 | 输入文件 | 放置位置 |
| --- | --- | --- |
| Hadoop WordCount | `stock_data.csv`（股票评论文本） | `bigdata-homework5/`，挂载为容器内 `/homework/` |
| MapReduce | `ccf_offline_stage1_train.csv` | `bigdata-project2/`，用于本地样本检查；Hadoop 流程另需上传 HDFS |
| Hive | `ccf_offline_stage1_train.csv`、`ccf_online_stage1_train.csv` | `bigdata-project3/dataset/`，Compose 挂载为 `/dataset/` |
| Spark | `ccf_offline_stage1_train.csv`、`ccf_online_stage1_train.csv`、`ccf_offline_stage1_test_revised.csv` | `bigdata-project4/data/raw/` |

优惠券数据使用天池 O2O 优惠券使用预测赛题的文件格式，需自行准备；股票评论输入也需自行提供。`.gitignore` 排除了原始 CSV 和数据目录，克隆后需要创建对应目录并放入数据。

### 2. 按模块运行

**MapReduce 本地样本检查**：从仓库根目录执行。脚本读取输入文件的前 50,000 行，通过 Mapper → 排序 → Reducer 管道展示结果，用于检查任务逻辑。

```bash
cd bigdata-project2
bash test_local.sh
```

完整 Hadoop 运行流程见 [MapReduce 说明](bigdata-project2/README.md)和[实验报告](bigdata-project2/实验报告.md)；[可视化脚本](bigdata-project2/visualize.py)读取已生成的结果文件绘图。原有 Hadoop 指南使用 `newhadoop` 镜像及固定的容器、挂载路径，运行前需按本机环境配置。

**Spark 三个任务**：从仓库根目录执行。先准备上述三个 CSV，再安装依赖并依次运行 RDD、SQL、MLlib 任务；SQL 商家比例分析依赖 RDD 输出。

```bash
cd bigdata-project4
python3 -m pip install -r requirements.txt
bash run.sh
```

分步执行方式见 [Spark 说明](bigdata-project4/README.md)。目录内保留 [Dockerfile](bigdata-project4/Dockerfile)和[容器运行脚本](bigdata-project4/docker_run.sh)；该脚本依赖已有的 `spark-bigdata` 镜像及 `/home/kleene/workspace` 挂载路径，使用时需适配本机目录。

**Hive**：在 `bigdata-project3/` 启动 [Docker Compose 服务](bigdata-project3/docker-compose.yml)，准备好数据后连接 Beeline，创建 `coupon_analysis` 数据库，再按顺序执行 [导入](bigdata-project3/scripts/task1_import_data.sql)、[基础查询](bigdata-project3/scripts/task2_basic_query.sql)、[聚合分析](bigdata-project3/scripts/task3_aggregation.sql)和[复杂查询](bigdata-project3/scripts/task4_complex_query.sql)。Compose 将脚本目录挂载到容器 `/scripts/`；首次运行前先建库，因为导入脚本开头已包含 `USE coupon_analysis`。详细配置与排错见 [Hive 说明](bigdata-project3/README.md)和[实验报告](bigdata-project3/实验报告.md)。

**Hadoop WordCount**：按 [模块报告](bigdata-homework5/README.md)配置伪分布式环境，将数据挂载到 `/homework/`，启动 HDFS / YARN 后执行 [run_cluster.sh](bigdata-homework5/run_cluster.sh)。报告保留 NameNode、ResourceManager 和 JobHistory 的查看方式与运行截图。

### 3. 环境版本

| 环境 | 仓库中的配置或记录 |
| --- | --- |
| Hadoop WordCount / MapReduce | 脚本与报告使用 Hadoop 3.4.0 |
| Hive | Compose 使用 Hive 2.3.2；NameNode / DataNode 镜像标签为 Hadoop 3.2.1。历史实验报告记载 Hadoop 2.7.4，复现时以实际镜像为准 |
| Hive Metastore | PostgreSQL，配置见 `docker-compose.yml` |
| Spark | PySpark、NumPy；Dockerfile 基于 Python 3.9，安装 Java 运行环境 |
| 可视化 | Python、matplotlib |

Spark 的 [requirements.txt](bigdata-project4/requirements.txt)没有锁定依赖版本；安装时需要确认 Python、Java 与 PySpark 的版本兼容性。实验报告和截图记录历史运行环境，当前目录结构与脚本提供复现入口。

## 仓库结构

```text
.
├── bigdata-homework5/       # HDFS / YARN / WordCount、脚本与截图
├── bigdata-project2/        # MapReduce 源码、结果、图表与报告
│   ├── src/                # 四项分析任务的 Mapper / Reducer
│   ├── output/             # 商家统计结果与四张可视化图
│   └── logs/               # 距离、间隔、折扣与活跃度结果
├── bigdata-project3/        # Hive 环境、查询与报告
│   ├── scripts/            # 四个 HiveQL 任务
│   ├── pictures/           # 查询结果截图
│   └── docker-compose.yml  # 服务编排
└── bigdata-project4/        # Spark RDD / SQL / MLlib
    ├── src/                # 三个 Python 任务
    ├── output/             # 统计与预测输出
    ├── pictures/           # 运行截图
    └── run.sh              # 顺序执行入口
```

课程：金融大数据技术 · 学期：2025 年
