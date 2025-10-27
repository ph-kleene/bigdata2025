#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务一 Reducer: 商家优惠券使用情况统计
输出格式: <Merchant_id> TAB <负样本数> TAB <普通消费数> TAB <正样本数>
"""

import sys
from collections import defaultdict

def main():
    current_key = None
    stats = defaultdict(int)
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            # 解析 key-value
            key, value = line.split('\t')
            
            # 如果遇到新的商家，输出上一个商家的统计
            if current_key and current_key != key:
                output_stats(current_key, stats)
                stats = defaultdict(int)
            
            # 累计统计
            current_key = key
            stats[value] += 1
            
        except Exception as e:
            continue
    
    # 输出最后一个商家
    if current_key:
        output_stats(current_key, stats)

def output_stats(key, stats):
    """输出统计结果"""
    # 提取merchant_id和data_type
    negative = stats.get('negative', 0)
    normal = stats.get('normal', 0)
    positive = stats.get('positive', 0)
    
    # 输出格式: merchant_id_type TAB 负样本 TAB 普通消费 TAB 正样本
    print("{}	{}	{}	{}".format(key, negative, normal, positive))

if __name__ == '__main__':
    main()
