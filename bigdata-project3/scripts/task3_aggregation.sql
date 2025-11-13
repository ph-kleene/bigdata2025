-- ============================================
-- 任务三：数据聚合分析
-- ============================================

USE coupon_analysis;
SET hive.exec.mode.local.auto=true;
SET mapreduce.framework.name=local;

-- 3.1 商家周边活跃顾客数量统计
-- 统计每个商家在不同距离范围内的活跃消费者人数
SELECT 
    Merchant_id,
    SUM(CASE WHEN Distance = '0' THEN 1 ELSE 0 END) AS distance_0,
    SUM(CASE WHEN Distance = '1' THEN 1 ELSE 0 END) AS distance_1,
    SUM(CASE WHEN Distance = '2' THEN 1 ELSE 0 END) AS distance_2,
    SUM(CASE WHEN Distance = '3' THEN 1 ELSE 0 END) AS distance_3,
    SUM(CASE WHEN Distance = '4' THEN 1 ELSE 0 END) AS distance_4,
    SUM(CASE WHEN Distance = '5' THEN 1 ELSE 0 END) AS distance_5,
    SUM(CASE WHEN Distance = '6' THEN 1 ELSE 0 END) AS distance_6,
    SUM(CASE WHEN Distance = '7' THEN 1 ELSE 0 END) AS distance_7,
    SUM(CASE WHEN Distance = '8' THEN 1 ELSE 0 END) AS distance_8,
    SUM(CASE WHEN Distance = '9' THEN 1 ELSE 0 END) AS distance_9,
    SUM(CASE WHEN Distance = '10' THEN 1 ELSE 0 END) AS distance_10,
    SUM(CASE WHEN Distance = 'null' THEN 1 ELSE 0 END) AS distance_null
FROM ccf_offline_stage1_train
GROUP BY Merchant_id
LIMIT 20;


-- 3.2 商家正样本比例统计
-- 按正样本比例排序，给出正样本比例最高的前十个商家
SELECT 
    Merchant_id,
    ROUND(positive_samples / (negative_samples + normal_consumption + positive_samples), 4) AS positive_ratio,
    positive_samples,
    (negative_samples + normal_consumption + positive_samples) AS total_samples
FROM online_consumption_table
WHERE (negative_samples + normal_consumption + positive_samples) > 0
ORDER BY positive_ratio DESC
LIMIT 10;
