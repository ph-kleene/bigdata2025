#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务四 Mapper: 分析折扣率对优惠券使用的影响
输出格式: <折扣率等级> TAB <是否使用> TAB 1
"""

import sys

def parse_discount(discount_str):
    """
    解析折扣率字段，返回实际折扣力度
    - "0.8" -> 0.8 (打8折)
    - "200:20" -> 0.9 (满200减20，相当于9折)
    - "150:20:00" -> 0.867 (满150减20)
    - "fixed" -> 1.0 (固定金额，视为无折扣)
    """
    if discount_str == 'null' or discount_str == 'fixed':
        return 1.0
    
    if ':' in discount_str:
        # 满减类型
        try:
            parts = discount_str.split(':')
            full = float(parts[0])
            minus = float(parts[1])
            discount_rate = (full - minus) / full
            return discount_rate
        except:
            return 1.0
    else:
        # 直接折扣
        try:
            rate = float(discount_str)
            return rate
        except:
            return 1.0

def get_discount_level(discount_rate):
    """将折扣率分级 - 数值越小折扣越大"""
    if discount_rate >= 0.95:
        return "极小折扣(95%-100%)"
    elif discount_rate >= 0.85:
        return "小折扣(85%-95%)"
    elif discount_rate >= 0.70:
        return "中等折扣(70%-85%)"
    elif discount_rate >= 0.50:
        return "大折扣(50%-70%)"
    else:
        return "超大折扣(<50%)"

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
            coupon_id = fields[2]
            discount_rate_str = fields[3]
            date_received = fields[5]
            date = fields[6]
            
            # 只分析有优惠券的数据
            if coupon_id == 'null' or date_received == 'null':
                continue
            
            # 解析折扣率
            discount_rate = parse_discount(discount_rate_str)
            discount_level = get_discount_level(discount_rate)
            
            # 判断是否使用
            is_used = "1" if date != 'null' else "0"
            
            # 输出: 折扣等级 TAB 是否使用 TAB 1
            print("{}\t{}\t1".format(discount_level, is_used))
                
        except Exception as e:
            continue

if __name__ == '__main__':
    main()
