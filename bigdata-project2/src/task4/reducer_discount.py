#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务四 Reducer: 统计不同折扣率的使用情况
输出格式: <折扣等级> TAB <总发放数> TAB <使用数> TAB <使用率>
"""

import sys
from collections import defaultdict

def main():
    # {discount_level: {total: 总数, used: 使用数}}
    stats = defaultdict(lambda: {'total': 0, 'used': 0})
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            parts = line.split('\t')
            if len(parts) != 3:
                continue
                
            discount_level = parts[0]
            is_used = parts[1]
            count = int(parts[2])
            
            stats[discount_level]['total'] += count
            if is_used == '1':
                stats[discount_level]['used'] += count
                
        except Exception as e:
            continue
    
    # 输出统计结果，按折扣力度排序
    discount_order = [
        "超大折扣(<50%)",
        "大折扣(50%-70%)",
        "中等折扣(70%-85%)",
        "小折扣(85%-95%)",
        "极小折扣(95%-100%)"
    ]
    
    for level in discount_order:
        if level in stats:
            total = stats[level]['total']
            used = stats[level]['used']
            rate = (used / total * 100) if total > 0 else 0
            print("{}	{}	{}	{:.2f}%".format(level, total, used, rate))

if __name__ == '__main__':
    main()
