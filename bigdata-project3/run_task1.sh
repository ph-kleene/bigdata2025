#!/bin/bash
# Hive SQL执行脚本

echo "开始执行任务一：创建表和导入数据"
echo "======================================"

docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 << 'EOF'

-- 创建数据库
CREATE DATABASE IF NOT EXISTS coupon_analysis;
USE coupon_analysis;

-- 创建线下消费表
DROP TABLE IF EXISTS ccf_offline_stage1_train;
CREATE TABLE ccf_offline_stage1_train (
    User_id STRING,
    Merchant_id STRING,
    Coupon_id STRING,
    Discount_rate STRING,
    Distance STRING,
    Date_received STRING,
    Date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- 创建线上消费表
DROP TABLE IF EXISTS ccf_online_stage1_train;
CREATE TABLE ccf_online_stage1_train (
    User_id STRING,
    Merchant_id STRING,
    Action STRING,
    Coupon_id STRING,
    Discount_rate STRING,
    Date_received STRING,
    Date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- 显示表
SHOW TABLES;

-- 加载数据
LOAD DATA LOCAL INPATH '/dataset/ccf_offline_stage1_train.csv' INTO TABLE ccf_offline_stage1_train;
LOAD DATA LOCAL INPATH '/dataset/ccf_online_stage1_train.csv' INTO TABLE ccf_online_stage1_train;

-- 验证数据加载 - 查看前5条记录
SELECT * FROM ccf_offline_stage1_train LIMIT 5;
SELECT * FROM ccf_online_stage1_train LIMIT 5;

-- 统计记录总数
SELECT COUNT(*) AS offline_count FROM ccf_offline_stage1_train;
SELECT COUNT(*) AS online_count FROM ccf_online_stage1_train;

EOF

echo "======================================"
echo "任务一执行完成"
