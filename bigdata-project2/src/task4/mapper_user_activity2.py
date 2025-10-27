#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务四(扩展) Mapper 阶段2: 根据用户活跃度分组
输入: <User_id> TAB <领券数> TAB <使用数>
输出: <活跃度等级> TAB <核销率>
"""

import sys

def get_activity_level(received_count):
    """根据领券数量判断用户活跃度等级"""
    if received_count >= 50:
        return "高频用户(>=50券)"
    elif received_count >= 20:
        return "中频用户(20-49券)"
    elif received_count >= 10:
        return "低频用户(10-19券)"
    else:
        return "偶尔用户(1-9券)"

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            parts = line.split('\t')
            if len(parts) != 3:
                continue
                
            user_id = parts[0]
            received = int(parts[1])
            used = int(parts[2])
            
            # 计算核销率
            usage_rate = (used / received) if received > 0 else 0
            
            # 获取活跃度等级
            level = get_activity_level(received)
            
            # 输出: 活跃度等级 TAB 领券数 TAB 使用数
            print("{}\t{}\t{}".format(level, received, used))
                
        except Exception as e:
            continue

if __name__ == '__main__':
    main()
