# Hive实验指南

## 环境准备

### 1. 启动Docker Compose环境

本实验使用Docker Compose管理多容器Hive集群环境。

```bash
# 进入项目目录
cd workspace/bigdata2025/bigdata-project3

# 启动所有容器
docker-compose up -d

# 查看容器状态
docker-compose ps
```

**容器组成**：
- `namenode`: Hadoop NameNode
- `datanode`: Hadoop DataNode
- `hive-server`: Hive Server2
- `hive-metastore`: Hive Metastore服务
- `hive-metastore-postgresql`: PostgreSQL元数据库

### 2. 连接到Hive

使用Beeline客户端连接：

```bash
# 方式一：在容器内连接
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000

# 方式二：执行SQL文件
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/scripts/task1_import_data.sql
```

## 实验步骤

### 任务一：导入数据

执行数据导入脚本：

```bash
docker cp scripts/task1_import_data.sql hive-server:/tmp/
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/task1_import_data.sql
```

**需要截图的步骤**：
1. `SELECT * FROM ccf_offline_stage1_train LIMIT 5;` → `task1_offline_limit5.png`
2. `SELECT * FROM ccf_online_stage1_train LIMIT 5;` → `task1_online_limit5.png`
3. `SELECT COUNT(*) FROM ccf_offline_stage1_train;` → `task1_offline_count.png`
4. `SELECT COUNT(*) FROM ccf_online_stage1_train;` → `task1_online_count.png`

### 任务二：基本数据查询

执行基本查询脚本：

```bash
docker cp scripts/task2_basic_query.sql hive-server:/tmp/
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/task2_basic_query.sql
```

**需要截图的步骤**：
1. 用户行为数量统计 → `task2_1_behavior_count.png`
2. `SELECT * FROM online_consumption_table LIMIT 20;` → `task2_2_merchant_coupon.png`

### 任务三：数据聚合分析

执行聚合分析脚本：

```bash
docker cp scripts/task3_aggregation.sql hive-server:/tmp/
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/task3_aggregation.sql
```

**需要截图的步骤**：
1. 商家周边活跃顾客数量统计 → `task3_1_merchant_distance.png`
2. 商家正样本比例Top10 → `task3_2_positive_ratio.png`

### 任务四：复杂查询与分析

执行复杂查询脚本（包含两个子任务的所有方法）：

```bash
docker cp scripts/task4_complex_query.sql hive-server:/tmp/
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/task4_complex_query.sql
```

**需要截图的步骤**：
1. 优惠券使用时间统计 → `task4_1_coupon_time.png`
2. 优惠券折扣率统计（方法一）→ `task4_2_discount_rate_1.png`
3. 优惠券折扣率统计（方法二）→ `task4_2_discount_rate_2.png`

**注意**：任务4.2有两种实现方法，需要分别截图。

## 截图保存位置

所有截图请保存到：`workspace/bigdata2025/bigdata-project3/pictures/`

## 实验文件说明

### SQL脚本

所有SQL脚本位于 `scripts/` 目录：

- **task1_import_data.sql**: 创建数据库、表，导入数据，验证数据
- **task2_basic_query.sql**: 用户行为统计，创建线上消费表
- **task3_aggregation.sql**: 距离分布统计，正样本比例排序
- **task4_complex_query.sql**: 优惠券使用时间分析，折扣率统计（含两种方法）

所有脚本都已配置本地执行模式（`SET hive.exec.mode.local.auto=true`），可直接执行。

### 配置文件

- **docker-compose.yml**: Docker Compose编排配置
- **hadoop.env**: Hadoop环境变量配置

### 数据文件

数据文件存放在 `dataset/` 目录：
- `ccf_offline_stage1_train.csv`: 线下优惠券数据
- `ccf_online_stage1_train.csv`: 线上优惠券数据

## 注意事项

1. **数据格式**：CSV文件中的null值是字符串 "null"，需要使用 `!= 'null'` 而非 `IS NOT NULL`
2. **保留字处理**：`Date` 是保留字，查询时需要使用反引号：`` `Date` ``
3. **本地模式**：所有脚本都已配置本地执行模式，避免MapReduce集群配置问题
4. **执行顺序**：建议按任务顺序执行，因为后续任务可能依赖前面创建的表
5. **临时表清理**：任务4.1会创建临时表 `temp_coupon_usage`，如需重新执行可先删除

## 环境管理

```bash
# 停止所有容器
docker-compose down

# 重新启动
docker-compose up -d

# 查看日志
docker-compose logs -f hive-server

# 清理所有数据（包括元数据）
docker-compose down -v
```

## 常用命令

```sql
-- 查看所有数据库
SHOW DATABASES;

-- 使用数据库
USE coupon_analysis;

-- 查看所有表
SHOW TABLES;

-- 查看表结构
DESCRIBE ccf_offline_stage1_train;

-- 删除表（如需重新创建）
DROP TABLE IF EXISTS table_name;
```
