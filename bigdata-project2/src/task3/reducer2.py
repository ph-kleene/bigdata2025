#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务三 Reducer (阶段2): 计算平均使用时间间隔
输出格式: <Coupon_id> TAB <平均间隔天数>
"""

import sys
from collections import defaultdict
from datetime import datetime

def parse_date(date_str):
    """解析日期字符串 YYYYMMDD"""
    try:
        return datetime.strptime(date_str, '%Y%m%d')
    except:
        return None

def main():
    current_coupon = None
    intervals = []
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            parts = line.split('\t')
            if len(parts) != 3:
                continue
                
            coupon_id, date_received, date_used = parts
            
            # 如果遇到新优惠券,输出上一个的统计
            if current_coupon and current_coupon != coupon_id:
                if len(intervals) > 0:
                    avg_interval = sum(intervals) / len(intervals)
                    print("{}	{:.2f}".format(current_coupon, avg_interval))
                intervals = []
            
            # 计算时间间隔
            current_coupon = coupon_id
            d1 = parse_date(date_received)
            d2 = parse_date(date_used)
            
            if d1 and d2:
                interval = (d2 - d1).days
                if interval >= 0:  # 只统计正间隔
                    intervals.append(interval)
                    
        except Exception as e:
            continue
    
        # 输出最后一个优惠券
    if current_coupon and len(intervals) > 0:
        avg_interval = sum(intervals) / len(intervals)
        print("{}	{:.2f}".format(current_coupon, avg_interval))

if __name__ == '__main__':
    main()
