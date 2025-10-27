#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务四(扩展) Reducer 阶段2: 统计不同活跃度用户的核销率
输出格式: <活跃度等级> TAB <用户数> TAB <总领券数> TAB <总使用数> TAB <核销率>
"""

import sys
from collections import defaultdict

def main():
    # {level: {users: set, received: 总领券, used: 总使用}}
    stats = defaultdict(lambda: {'users': set(), 'received': 0, 'used': 0})
    
    current_level = None
    level_received = 0
    level_used = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            parts = line.split('\t')
            if len(parts) != 3:
                continue
                
            level = parts[0]
            received = int(parts[1])
            used = int(parts[2])
            
            # 如果遇到新等级，输出上一个等级的统计
            if current_level and current_level != level:
                if level_received > 0:
                    rate = (level_used / level_received * 100)
                    print("{}	{}	{}	{:.2f}%".format(current_level, level_received, level_used, rate))
                level_received = 0
                level_used = 0
            
            # 累计统计
            current_level = level
            level_received += received
            level_used += used
                
        except Exception as e:
            continue
    
    # 输出最后一个等级
    if current_level and level_received > 0:
        rate = (level_used / level_received * 100)
        print("{}	{}	{}	{:.2f}%".format(current_level, level_received, level_used, rate))

if __name__ == '__main__':
    main()
