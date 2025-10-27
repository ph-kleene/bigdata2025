#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务一 Mapper: 商家优惠券使用情况统计
分类规则:
- 负样本 (negative): Date=null && Coupon_id != null (领券未使用)
- 普通消费 (normal): Date!=null && Coupon_id = null (未领券直接消费)
- 正样本 (positive): Date!=null && Coupon_id != null (领券并使用)
"""

import sys

def classify_sample(coupon_id, date):
    """分类样本类型"""
    if date != 'null' and coupon_id != 'null':
        return 'positive'  # 正样本
    elif date == 'null' and coupon_id != 'null':
        return 'negative'  # 负样本
    elif date != 'null' and coupon_id == 'null':
        return 'normal'    # 普通消费
    else:
        return None  # 无效样本

def main():
    # 跳过CSV头部
    header = sys.stdin.readline()
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            # 解析CSV行
            fields = line.split(',')
            
            # offline 数据: User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
            # online 数据: User_id,Merchant_id,Action,Coupon_id,Discount_rate,Date_received,Date
            
            # 判断是 offline 还是 online (通过字段数量)
            if len(fields) == 7:
                # offline 数据
                merchant_id = fields[1]
                coupon_id = fields[2]
                date = fields[6]
                data_type = 'offline'
            elif len(fields) == 8:
                # online 数据  
                merchant_id = fields[1]
                coupon_id = fields[3]
                date = fields[6]
                data_type = 'online'
            else:
                continue
            
            # 分类样本
            sample_type = classify_sample(coupon_id, date)
            
            if sample_type:
                # 输出: key = merchant_id_datatype, value = sample_type
                print("{}_{}	{}".format(merchant_id, data_type, sample_type))
                
        except Exception as e:
            # 忽略错误行
            continue

if __name__ == '__main__':
    main()
