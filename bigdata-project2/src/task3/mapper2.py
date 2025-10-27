#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务三 Mapper (阶段2): 计算优惠券使用的时间间隔
读取原始数据,对于频繁使用的优惠券,输出时间间隔
输出格式: <Coupon_id> TAB <间隔天数>
"""

import sys
from datetime import datetime

def main():
    # 首先从命令行参数或环境变量获取频繁优惠券列表
    # 为了简化,这里直接处理所有数据,在reducer中过滤
    
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
            coupon_id = fields[2]
            date_received = fields[5]
            date = fields[6]
            
            # 只处理使用了的优惠券
            if coupon_id != 'null' and date_received != 'null' and date != 'null':
                # 输出优惠券ID和两个日期
                print("{}\t{}\t{}".format(coupon_id, date_received, date))
                
        except Exception as e:
            continue

if __name__ == '__main__':
    main()
