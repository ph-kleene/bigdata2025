-- ============================================
-- 任务四：复杂查询与分析
-- ============================================

USE coupon_analysis;
SET hive.exec.mode.local.auto=true;
SET mapreduce.framework.name=local;

-- ============================================
-- 4.1 优惠券使用时间统计
-- ============================================
-- 统计使用次数大于总使用次数1%的优惠券，计算从领取到使用的平均间隔
-- 由于Hive 2.3对子查询支持有限，采用临时表方式实现

-- 步骤1：创建临时表存储优惠券使用统计
DROP TABLE IF EXISTS temp_coupon_usage;
CREATE TABLE temp_coupon_usage AS
SELECT 
    Coupon_id,
    COUNT(*) AS usage_count,
    AVG(DATEDIFF(
        FROM_UNIXTIME(UNIX_TIMESTAMP(`Date`, 'yyyyMMdd')),
        FROM_UNIXTIME(UNIX_TIMESTAMP(Date_received, 'yyyyMMdd'))
    )) AS avg_interval
FROM ccf_offline_stage1_train
WHERE Coupon_id != 'null' 
    AND `Date` != 'null' 
    AND Date_received != 'null'
GROUP BY Coupon_id;

-- 步骤2：计算总使用次数的1%作为阈值
SELECT SUM(usage_count) * 0.01 AS threshold FROM temp_coupon_usage;
-- 结果：449.66

-- 步骤3：查询超过阈值的优惠券
SELECT 
    Coupon_id,
    usage_count,
    ROUND(avg_interval, 2) AS avg_interval_days
FROM temp_coupon_usage
WHERE usage_count > 449.66
ORDER BY usage_count DESC;


-- ============================================
-- 4.2 优惠券折扣率统计
-- ============================================
-- 统计使用率前十的优惠券及其折扣率
-- 
-- 字段理解说明：
-- 实验要求"Coupon_id缺失项不计入总使用次数"存在两种理解：
-- 理解1：总消费次数 = 所有消费记录
-- 理解2：总消费次数 = 仅使用优惠券的消费记录
-- 本脚本提供两种实现方式

-- ========== 方法一：总消费次数包括所有消费记录 ==========
-- 概念说明：
-- - 使用次数：该优惠券被实际使用的次数（领取且消费）
-- - 总消费次数：所有消费记录数（使用任何优惠券的消费 + 不使用优惠券的消费）
-- - 使用率 = 该优惠券使用次数 / 总消费次数

-- 步骤1：计算总消费次数
SELECT COUNT(*) AS total_consumption 
FROM ccf_offline_stage1_train 
WHERE `Date` != 'null';
-- 结果：463717

-- 步骤2：统计各优惠券使用率并计算折扣率
WITH coupon_usage AS (
    SELECT 
        Coupon_id,
        MAX(Discount_rate) AS Discount_rate,
        COUNT(*) AS used_count
    FROM ccf_offline_stage1_train
    WHERE Coupon_id != 'null' 
        AND `Date` != 'null'
    GROUP BY Coupon_id
)
SELECT 
    Coupon_id,
    ROUND(used_count / 463717, 4) AS usage_rate,
    used_count,
    CASE 
        WHEN Discount_rate LIKE '%:%' THEN 
            CONCAT(ROUND((1 - CAST(SPLIT(Discount_rate, ':')[1] AS FLOAT) / CAST(SPLIT(Discount_rate, ':')[0] AS FLOAT)) * 100, 2), '%')
        ELSE 
            CONCAT(ROUND((1 - CAST(Discount_rate AS FLOAT)) * 100, 2), '%')
    END AS discount_rate_percent,
    Discount_rate AS discount_type
FROM coupon_usage
ORDER BY usage_rate DESC
LIMIT 10;


-- ========== 方法二：总消费次数仅包括使用优惠券的消费 ==========
-- 概念说明：
-- - 使用次数：该优惠券被实际使用的次数
-- - 总消费次数：所有使用了优惠券的消费记录数（Coupon_id != 'null' 且 Date != 'null'）
-- - 使用率 = 该优惠券使用次数 / 使用优惠券的总消费次数

-- 步骤1：计算使用优惠券的总消费次数
SELECT COUNT(*) AS total_coupon_consumption 
FROM ccf_offline_stage1_train 
WHERE `Date` != 'null' AND Coupon_id != 'null';
-- 结果：44966

-- 步骤2：统计各优惠券使用率并计算折扣率
WITH coupon_usage AS (
    SELECT 
        Coupon_id,
        MAX(Discount_rate) AS Discount_rate,
        COUNT(*) AS used_count
    FROM ccf_offline_stage1_train
    WHERE Coupon_id != 'null' 
        AND `Date` != 'null'
    GROUP BY Coupon_id
)
SELECT 
    Coupon_id,
    ROUND(used_count / 44966, 4) AS usage_rate,
    used_count,
    CASE 
        WHEN Discount_rate LIKE '%:%' THEN 
            CONCAT(ROUND((1 - CAST(SPLIT(Discount_rate, ':')[1] AS FLOAT) / CAST(SPLIT(Discount_rate, ':')[0] AS FLOAT)) * 100, 2), '%')
        ELSE 
            CONCAT(ROUND((1 - CAST(Discount_rate AS FLOAT)) * 100, 2), '%')
    END AS discount_rate_percent,
    Discount_rate AS discount_type
FROM coupon_usage
ORDER BY usage_rate DESC
LIMIT 10;
