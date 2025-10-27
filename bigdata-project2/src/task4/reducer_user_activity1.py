#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务四(扩展) Reducer 阶段1: 统计每个用户的活跃度
输出格式: <User_id> TAB <领券数> TAB <使用数>
"""

import sys
from collections import defaultdict

def main():
    current_user = None
    received_count = 0
    used_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            parts = line.split('\t')
            if len(parts) != 3:
                continue
                
            user_id = parts[0]
            action_type = parts[1]
            count = int(parts[2])
            
            # 如果遇到新用户，输出上一个用户的统计
            if current_user and current_user != user_id:
                print("{}\t{}\t{}".format(current_user, received_count, used_count))
                received_count = 0
                used_count = 0
            
            # 累计统计
            current_user = user_id
            if action_type == 'received':
                received_count += count
            elif action_type == 'used':
                used_count += count
                
        except Exception as e:
            continue
    
    # 输出最后一个用户
    if current_user:
        print("{}\t{}\t{}".format(current_user, received_count, used_count))

if __name__ == '__main__':
    main()
