#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务二 Mapper: 商家距离统计
统计每个商家不同距离的活跃消费者人数（需要去重）
"""

import sys

def main():
    # 跳过CSV头部
    header = sys.stdin.readline()
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            # offline 数据: User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
            fields = line.split(',')
            
            if len(fields) != 7:
                continue
            
            user_id = fields[0]
            merchant_id = fields[1]
            distance = fields[4]
            date = fields[6]
            
            # 只统计有消费行为的记录 (Date != null)
            if date != 'null':
                # 输出: key = merchant_id_distance, value = user_id
                print("{}_{}\t{}".format(merchant_id, distance, user_id))
                
        except Exception as e:
            continue

if __name__ == '__main__':
    main()
