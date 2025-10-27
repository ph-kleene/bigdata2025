#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据可视化脚本 - 为所有MapReduce任务生成图表
"""

import matplotlib
matplotlib.use('Agg')  # 无GUI模式
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

# 使用默认字体，避免字体查找卡顿
plt.rcParams['axes.unicode_minus'] = False

def visualize_task1():
    """任务一：商家优惠券使用情况 - 前20个商家的柱状图"""
    print("生成任务一可视化...")
    
    with open('output/task1/result.txt', 'r') as f:
        lines = f.readlines()[:30]  # 读取前30个商家
    
    merchants = []
    negative = []
    normal = []
    positive = []
    
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) == 4:
            merchants.append(parts[0].replace('_offline', ''))
            negative.append(int(parts[1]))
            normal.append(int(parts[2]))
            positive.append(int(parts[3]))
    
    # 创建堆叠柱状图
    x = np.arange(len(merchants))
    width = 0.8
    
    fig, ax = plt.subplots(figsize=(15, 6))
    p1 = ax.bar(x, negative, width, label='Negative (Received but not used)', color='#ff7f0e')
    p2 = ax.bar(x, normal, width, bottom=negative, label='Normal (Consumed without coupon)', color='#2ca02c')
    p3 = ax.bar(x, positive, width, bottom=np.array(negative)+np.array(normal), 
                label='Positive (Received and used)', color='#1f77b4')
    
    ax.set_xlabel('Merchant ID', fontsize=12)
    ax.set_ylabel('Count', fontsize=12)
    ax.set_title('Task 1: Merchant Coupon Usage Statistics (Top 30)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(merchants, rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('output/task1_visualization.png', dpi=300, bbox_inches='tight')
    print("✓ 任务一可视化已保存: output/task1_visualization.png")
    plt.close()

def visualize_task2():
    """任务二：商家距离统计 - 选取几个代表性商家"""
    print("生成任务二可视化...")
    
    with open('logs/task2_result.txt', 'r') as f:
        lines = f.readlines()
    
    # 选择有多个距离级别的商家
    selected_merchants = {}
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            merchant = parts[0]
            distances = parts[1]
            # 选择有3个以上距离级别的商家
            if distances.count(',') >= 2:
                dist_dict = {}
                for item in distances.split(','):
                    d, c = item.split(':')
                    dist_dict[d] = int(c)
                selected_merchants[merchant] = dist_dict
                if len(selected_merchants) >= 6:
                    break
    
    # 创建子图
    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    axes = axes.flatten()
    
    for idx, (merchant, dist_dict) in enumerate(selected_merchants.items()):
        ax = axes[idx]
        distances = sorted(dist_dict.keys(), key=lambda x: (x=='null', 999 if x=='null' else int(x)))
        counts = [dist_dict[d] for d in distances]
        
        ax.bar(range(len(distances)), counts, color='steelblue')
        ax.set_xlabel('Distance', fontsize=10)
        ax.set_ylabel('User Count', fontsize=10)
        ax.set_title('Merchant {}'.format(merchant), fontsize=11, fontweight='bold')
        ax.set_xticks(range(len(distances)))
        ax.set_xticklabels(distances, rotation=45)
        ax.grid(axis='y', alpha=0.3)
    
    plt.suptitle('Task 2: Distance Distribution for Selected Merchants', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('output/task2_visualization.png', dpi=300, bbox_inches='tight')
    print("✓ 任务二可视化已保存: output/task2_visualization.png")
    plt.close()

def visualize_task3():
    """任务三：优惠券使用间隔分布"""
    print("生成任务三可视化...")
    
    with open('logs/task3_result.txt', 'r') as f:
        lines = f.readlines()
    
    intervals = []
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            try:
                interval = float(parts[1])
                intervals.append(interval)
            except:
                pass
    
    # 创建分布图
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # 直方图
    ax1.hist(intervals, bins=50, color='skyblue', edgecolor='black', alpha=0.7)
    ax1.set_xlabel('Interval Days', fontsize=12)
    ax1.set_ylabel('Frequency', fontsize=12)
    ax1.set_title('Distribution of Coupon Usage Intervals', fontsize=13, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    ax1.axvline(np.median(intervals), color='red', linestyle='--', label='Median: {:.1f}'.format(np.median(intervals)))
    ax1.axvline(np.mean(intervals), color='green', linestyle='--', label='Mean: {:.1f}'.format(np.mean(intervals)))
    ax1.legend()
    
    # 箱线图
    ax2.boxplot(intervals, vert=True)
    ax2.set_ylabel('Interval Days', fontsize=12)
    ax2.set_title('Boxplot of Usage Intervals', fontsize=13, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    
    # 添加统计信息
    stats_text = 'Stats:\nMean: {:.2f}\nMedian: {:.2f}\nStd: {:.2f}\nMin: {:.2f}\nMax: {:.2f}'.format(
        np.mean(intervals), np.median(intervals), np.std(intervals), np.min(intervals), np.max(intervals)
    )
    ax2.text(1.5, np.percentile(intervals, 75), stats_text, fontsize=10, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.suptitle('Task 3: Coupon Usage Interval Analysis', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('output/task3_visualization.png', dpi=300, bbox_inches='tight')
    print("✓ 任务三可视化已保存: output/task3_visualization.png")
    plt.close()

def visualize_task4():
    """任务四：影响因素分析"""
    print("生成任务四可视化...")
    
    # 读取折扣率数据
    with open('logs/task4_discount_result.txt', 'r') as f:
        discount_lines = f.readlines()
    
    discount_levels = []
    discount_rates = []
    discount_totals = []
    discount_used = []
    
    for line in discount_lines:
        parts = line.strip().split('\t')
        if len(parts) == 4:
            discount_levels.append(parts[0])
            discount_totals.append(int(parts[1]))
            discount_used.append(int(parts[2]))
            rate_str = parts[3].replace('%', '')
            discount_rates.append(float(rate_str))
    
    # 读取用户活跃度数据
    with open('logs/task4_user_result.txt', 'r') as f:
        user_lines = f.readlines()
    
    user_levels = []
    user_rates = []
    user_totals = []
    
    for line in user_lines:
        parts = line.strip().split('\t')
        if len(parts) == 4:
            user_levels.append(parts[0])
            user_totals.append(int(parts[1]))
            rate_str = parts[3].replace('%', '')
            user_rates.append(float(rate_str))
    
    # 创建双子图
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # 折扣率影响 - 将中文改为英文
    discount_levels_en = []
    for level in discount_levels:
        if '超大折扣' in level:
            discount_levels_en.append('Huge (<50%)')
        elif '大折扣' in level:
            discount_levels_en.append('Large (50-70%)')
        elif '中等折扣' in level:
            discount_levels_en.append('Medium (70-85%)')
        elif '小折扣' in level:
            discount_levels_en.append('Small (85-95%)')
        elif '极小折扣' in level:
            discount_levels_en.append('Tiny (95-100%)')
        else:
            discount_levels_en.append(level)
    
    x1 = np.arange(len(discount_levels_en))
    ax1.bar(x1, discount_rates, color=['#d62728', '#ff7f0e', '#2ca02c', '#1f77b4', '#9467bd'])
    ax1.set_xlabel('Discount Level', fontsize=12)
    ax1.set_ylabel('Usage Rate (%)', fontsize=12)
    ax1.set_title('Task 4-1: Impact of Discount Rate', fontsize=13, fontweight='bold')
    ax1.set_xticks(x1)
    ax1.set_xticklabels(discount_levels_en, rotation=20, ha='right', fontsize=9)
    ax1.grid(axis='y', alpha=0.3)
    
    # 在柱子上添加数值
    for i, (rate, total) in enumerate(zip(discount_rates, discount_totals)):
        ax1.text(i, rate+0.5, '{:.1f}%\n(n={})'.format(rate, total), ha='center', fontsize=8)
    
    # 用户活跃度影响 - 将中文改为英文
    user_levels_en = []
    for level in user_levels:
        if '中频' in level:
            user_levels_en.append('Mid-freq (20-49)')
        elif '低频' in level:
            user_levels_en.append('Low-freq (10-19)')
        elif '偶尔' in level:
            user_levels_en.append('Occasional (1-9)')
        elif '高频' in level:
            user_levels_en.append('High-freq (>=50)')
        else:
            user_levels_en.append(level)
    
    x2 = np.arange(len(user_levels_en))
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    ax2.bar(x2, user_rates, color=colors)
    ax2.set_xlabel('User Activity Level', fontsize=12)
    ax2.set_ylabel('Usage Rate (%)', fontsize=12)
    ax2.set_title('Task 4-2: Impact of User Activity', fontsize=13, fontweight='bold')
    ax2.set_xticks(x2)
    ax2.set_xticklabels(user_levels_en, rotation=20, ha='right', fontsize=9)
    ax2.grid(axis='y', alpha=0.3)
    
    # 在柱子上添加数值
    for i, (rate, total) in enumerate(zip(user_rates, user_totals)):
        ax2.text(i, rate+2, '{:.1f}%\n(n={})'.format(rate, total), ha='center', fontsize=8)
    
    plt.tight_layout()
    plt.savefig('output/task4_visualization.png', dpi=300, bbox_inches='tight')
    print("✓ 任务四可视化已保存: output/task4_visualization.png")
    plt.close()

def main():
    print("=" * 50)
    print("开始生成所有任务的可视化图表...")
    print("=" * 50)
    
    visualize_task1()
    visualize_task2()
    visualize_task3()
    visualize_task4()
    
    print("\n" + "=" * 50)
    print("所有可视化图表生成完成！")
    print("=" * 50)

if __name__ == '__main__':
    main()
