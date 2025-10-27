#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务二 Reducer: 商家距离统计
输出格式: <Merchant_id> TAB <距离0:人数,距离1:人数,...>
"""

import sys
from collections import defaultdict

def main():
    current_merchant = None
    distance_users = defaultdict(set)  # {distance: set(users)}
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            # 解析 key-value
            key, user_id = line.split('\t')
            merchant_id, distance = key.rsplit('_', 1)
            
            # 如果遇到新商家，输出上一个商家的统计
            if current_merchant and current_merchant != merchant_id:
                output_stats(current_merchant, distance_users)
                distance_users = defaultdict(set)
            
            # 累计统计 (去重用户)
            current_merchant = merchant_id
            distance_users[distance].add(user_id)
            
        except Exception as e:
            continue
    
    # 输出最后一个商家
    if current_merchant:
        output_stats(current_merchant, distance_users)

def output_stats(merchant_id, distance_users):
    """输出统计结果"""
    # 按距离排序 (null放最后)
    distances = sorted(distance_users.keys(), key=lambda x: (x == 'null', 999 if x == 'null' else int(x)))
    
    # 构建输出字符串
    result = []
    for dist in distances:
        count = len(distance_users[dist])
        result.append("{}:{}".format(dist, count))
    
    print("{}\t{}".format(merchant_id, ','.join(result)))

if __name__ == '__main__':
    main()
