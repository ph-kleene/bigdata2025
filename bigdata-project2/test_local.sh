#!/bin/bash
# 本地测试脚本 - 使用样本数据测试所有MapReduce任务

SAMPLE_SIZE=50000
DATA_FILE="ccf_offline_stage1_train.csv"

echo "========================================="
echo "任务一：商家优惠券使用情况统计"
echo "========================================="
head -$SAMPLE_SIZE $DATA_FILE | \
    python3 src/task1/mapper.py | \
    sort | \
    python3 src/task1/reducer.py | \
    head -10
echo ""

echo "========================================="
echo "任务二：商家距离统计"
echo "========================================="
head -$SAMPLE_SIZE $DATA_FILE | \
    python3 src/task2/mapper.py | \
    sort | \
    python3 src/task2/reducer.py | \
    head -10
echo ""

echo "========================================="
echo "任务三：优惠券使用间隔统计"
echo "========================================="
head -$SAMPLE_SIZE $DATA_FILE | \
    python3 src/task3/mapper2.py | \
    sort | \
    python3 src/task3/reducer2.py | \
    sort -t$'\t' -k2 -n | \
    head -10
echo ""

echo "========================================="
echo "任务四-1：折扣率对优惠券使用的影响"
echo "========================================="
head -$SAMPLE_SIZE $DATA_FILE | \
    python3 src/task4/mapper_discount.py | \
    sort | \
    python3 src/task4/reducer_discount.py
echo ""

echo "========================================="
echo "任务四-2：用户活跃度对核销率的影响"
echo "========================================="
head -$SAMPLE_SIZE $DATA_FILE | \
    python3 src/task4/mapper_user_activity.py | \
    sort | \
    python3 src/task4/reducer_user_activity1.py | \
    python3 src/task4/mapper_user_activity2.py | \
    sort | \
    python3 src/task4/reducer_user_activity2.py
echo ""

echo "本地测试完成！"
