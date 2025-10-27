#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务四(扩展) Mapper: 用户活跃度对优惠券使用的影响分析
第一阶段：统计每个用户的领券次数和使用次数
输出格式: <User_id> TAB <领券标记> TAB 1
"""

import sys

def main():
    # 跳过标题行
    header = sys.stdin.readline()
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            fields = line.split(',')
            if len(fields) < 7:
                continue
            
            # 提取字段
            user_id = fields[0]
            coupon_id = fields[2]
            date_received = fields[5]
            date = fields[6]
            
            # 只分析优惠券相关行为(有领券记录)
            if coupon_id != 'null' and date_received != 'null':
                # 输出领券记录
                print("{}\treceived\t1".format(user_id))
                
                # 如果使用了，输出使用记录
                if date != 'null':
                    print("{}\tused\t1".format(user_id))
                
        except Exception as e:
            continue

if __name__ == '__main__':
    main()
