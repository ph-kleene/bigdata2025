-- ============================================
-- 任务二：基本数据查询
-- ============================================

USE coupon_analysis;
SET hive.exec.mode.local.auto=true;
SET mapreduce.framework.name=local;

-- 2.1 查询用户行为数量
-- 统计三种行为（点击、购买、领取）的总次数，按数量降序排列
SELECT 
    CASE 
        WHEN Action = '0' THEN '点击'
        WHEN Action = '1' THEN '购买'
        WHEN Action = '2' THEN '领取'
        ELSE '未知'
    END AS behavior,
    COUNT(*) AS total_count
FROM ccf_online_stage1_train
GROUP BY Action
ORDER BY total_count DESC;


-- 2.2 查询商家优惠券使用情况
-- 创建新表存储统计结果
CREATE TABLE IF NOT EXISTS online_consumption_table (
    Merchant_id STRING,
    negative_samples INT,
    normal_consumption INT,
    positive_samples INT
);

-- 统计每个商家的优惠券使用情况
-- 负样本：Date=null 且 Coupon_id != null（领取但未使用）
-- 普通消费：Date!=null 且 Coupon_id = null（无优惠券消费）
-- 正样本：Date!=null 且 Coupon_id != null（使用优惠券消费）
INSERT OVERWRITE TABLE online_consumption_table
SELECT 
    Merchant_id,
    SUM(CASE WHEN Date = 'null' AND Coupon_id != 'null' THEN 1 ELSE 0 END) AS negative_samples,
    SUM(CASE WHEN Date != 'null' AND Coupon_id = 'null' THEN 1 ELSE 0 END) AS normal_consumption,
    SUM(CASE WHEN Date != 'null' AND Coupon_id != 'null' THEN 1 ELSE 0 END) AS positive_samples
FROM ccf_online_stage1_train
GROUP BY Merchant_id;

-- 查看结果
SELECT * FROM online_consumption_table LIMIT 20;
