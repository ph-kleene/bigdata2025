-- ============================================
-- 任务一：导入数据
-- ============================================

USE coupon_analysis;
SET hive.exec.mode.local.auto=true;
SET mapreduce.framework.name=local;

-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS coupon_analysis;
USE coupon_analysis;

-- 2. 创建线下消费表（注意：Date是保留字，需要用反引号）
DROP TABLE IF EXISTS ccf_offline_stage1_train;
CREATE TABLE ccf_offline_stage1_train (
    User_id STRING,
    Merchant_id STRING,
    Coupon_id STRING,
    Discount_rate STRING,
    Distance STRING,
    Date_received STRING,
    `Date` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- 3. 创建线上消费表
DROP TABLE IF EXISTS ccf_online_stage1_train;
CREATE TABLE ccf_online_stage1_train (
    User_id STRING,
    Merchant_id STRING,
    Action STRING,
    Coupon_id STRING,
    Discount_rate STRING,
    Date_received STRING,
    `Date` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- 4. 配置Hive本地模式（用于执行聚合查询）
SET hive.exec.mode.local.auto=true;
SET hive.exec.mode.local.auto.inputbytes.max=50000000;
SET hive.exec.mode.local.auto.input.files.max=10;
SET mapreduce.framework.name=local;

-- 5. 加载数据
LOAD DATA LOCAL INPATH '/dataset/ccf_offline_stage1_train.csv' INTO TABLE ccf_offline_stage1_train;
LOAD DATA LOCAL INPATH '/dataset/ccf_online_stage1_train.csv' INTO TABLE ccf_online_stage1_train;

-- 6. 验证数据加载 - 查看前5条记录
SELECT * FROM ccf_offline_stage1_train LIMIT 5;
SELECT * FROM ccf_online_stage1_train LIMIT 5;

-- 7. 统计记录总数
SELECT COUNT(*) AS offline_count FROM ccf_offline_stage1_train;
SELECT COUNT(*) AS online_count FROM ccf_online_stage1_train;
