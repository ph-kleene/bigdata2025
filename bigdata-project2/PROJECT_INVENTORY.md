# 项目文件清单

> 以下规模与清单记录原课程实验。当前公开仓库不包含原始 CSV，也不包含带学生身份信息的 PDF 导出；源码、Markdown 报告和汇总结果保留。

## 📂 目录结构

### 源代码 (src/)
```
src/
├── task1/              # 任务一：商家优惠券使用情况统计
│   ├── mapper.py      # 分类样本类型
│   └── reducer.py     # 汇总统计
├── task2/              # 任务二：商家距离统计
│   ├── mapper.py      # 提取商家-距离-用户
│   └── reducer.py     # 去重统计
├── task3/              # 任务三：优惠券使用间隔统计
│   ├── mapper2.py     # 提取使用记录
│   └── reducer2.py    # 计算平均间隔
└── task4/              # 任务四：影响因素分析
    ├── mapper_discount.py          # 折扣率分析Mapper
    ├── reducer_discount.py         # 折扣率分析Reducer
    ├── mapper_user_activity.py     # 用户活跃度Mapper(阶段1)
    ├── reducer_user_activity1.py   # 用户活跃度Reducer(阶段1)
    ├── mapper_user_activity2.py    # 用户活跃度Mapper(阶段2)
    └── reducer_user_activity2.py   # 用户活跃度Reducer(阶段2)
```

### 数据文件
- `ccf_offline_stage1_train.csv` (43MB) - 原始训练数据

### 结果文件 (output/ 和 logs/)
- `output/task1/result.txt` (8018行) - 商家优惠券统计结果
- `logs/task2_result.txt` (424行) - 商家距离统计结果
- `logs/task3_result.txt` (5462行) - 优惠券间隔统计结果
- `logs/task4_discount_result.txt` (5行) - 折扣率影响分析
- `logs/task4_user_result.txt` (4行) - 用户活跃度影响分析

### 可视化图表 (output/)
- `task1_visualization.png` (169KB) - 商家优惠券使用堆叠柱状图
- `task2_visualization.png` (208KB) - 商家距离分布子图
- `task3_visualization.png` (205KB) - 使用间隔直方图和箱线图
- `task4_visualization.png` (299KB) - 折扣率和用户活跃度对比图

### 脚本文件
- `visualize.py` - 数据可视化脚本
- `test_local.sh` - 本地测试脚本
- `run_task1.sh` - 任务一运行脚本(Hadoop)
- `run_all_tasks.sh` - 批量运行脚本(Hadoop)

### 文档文件
- `README.md` - 项目说明文档
- `PLAN.md` - 实验计划
- `实验2-MapReduce.md` - 实验要求
- `实验报告.md` - 详细实验报告
- `RESULTS_SUMMARY.md` - 结果汇总
- `PROJECT_COMPLETION.md` - 项目完成报告
- `PROJECT_INVENTORY.md` - 本文件(项目清单)

## ✅ 文件完整性检查

- [x] 12个Python MapReduce脚本
- [x] 1个可视化脚本
- [x] 3个Shell脚本
- [x] 5个结果文件
- [x] 4张可视化图表
- [x] 7个文档文件
- [ ] 原始数据文件需在本地自行准备

## 📊 数据统计

| 类型 | 数量 | 总大小 |
|------|------|--------|
| Python脚本 | 13 | ~15KB |
| Shell脚本 | 3 | ~5KB |
| 结果文件 | 5 | ~260KB |
| 可视化图表 | 4 | ~881KB |
| 文档文件 | 7 | ~30KB |
| 原始数据 | 1 | 43MB |

**总计**: 33个文件, ~44MB

---
**最后更新**: 2025年10月27日
