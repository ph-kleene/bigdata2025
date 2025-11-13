# MapReduce 优惠券数据分析实验

> 基于Hadoop MapReduce的O2O优惠券使用行为分析  
> **实验日期**: 2025年10月27日  
> **状态**: ✅ 全部完成

## 📋 项目概述

本项目使用Hadoop MapReduce框架对O2O优惠券数据集进行分布式分析，实现了4个核心分析任务，处理了75万+条记录，并生成了完整的数据洞察报告。

## 🎯 实验目标

- [x] 掌握MapReduce编程模型
- [x] 学习Hadoop Streaming API使用
- [x] 实践大数据分析流程
- [x] 提取商业价值洞察

## 🚀 快速开始

### 环境要求
- Docker (newhadoop镜像)
- Python 3.5+
- Hadoop 3.4.0
- matplotlib (可视化)

### 运行所有任务

```bash
# 1. 启动Hadoop容器
docker start hadoop-project2

# 2. 本地测试 (推荐)
cd /home/kleene/workspace/bigdata-project2
./test_local.sh

# 3. 生成可视化
python3 visualize.py
```

## 📊 实验任务

### 任务一：商家优惠券使用情况统计
**目标**: 分析每个商家的负样本、普通消费和正样本数量

- **Mapper**: `src/task1/mapper.py`
- **Reducer**: `src/task1/reducer.py`
- **结果**: `output/task1/result.txt` (8018个商家)
- **可视化**: `output/task1_visualization.png`

**核心逻辑**:
- 负样本: 领券未使用
- 普通消费: 未领券直接消费
- 正样本: 领券并使用

### 任务二：商家距离统计
**目标**: 统计每个商家不同距离级别的活跃用户数(去重)

- **Mapper**: `src/task2/mapper.py`
- **Reducer**: `src/task2/reducer.py` (使用set()去重)
- **结果**: `logs/task2_result.txt`
- **可视化**: `output/task2_visualization.png`

### 任务三：优惠券使用间隔统计
**目标**: 计算优惠券从领取到使用的平均天数

- **Mapper**: `src/task3/mapper2.py`
- **Reducer**: `src/task3/reducer2.py`
- **结果**: `logs/task3_result.txt` (5462个优惠券)
- **可视化**: `output/task3_visualization.png`

**关键发现**:
- 平均间隔: 7天
- 中位数: 4天
- 建议优惠券有效期: 7-14天

### 任务四：影响因素分析
**目标**: 分析折扣率和用户活跃度对核销率的影响

#### 4.1 折扣率分析
- **文件**: `mapper_discount.py`, `reducer_discount.py`
- **结果**: `logs/task4_discount_result.txt`

**核心洞察**:
```
极小折扣(95-100%)   核销率 13.84% ⭐ (最高)
中等折扣(70-85%)    核销率 8.94%
小折扣(85-95%)      核销率 3.64% (最低)
```

#### 4.2 用户活跃度分析 (两阶段MapReduce)
- **文件**: `mapper_user_activity.py`, `reducer_user_activity1.py`, `mapper_user_activity2.py`, `reducer_user_activity2.py`
- **结果**: `logs/task4_user_result.txt`

**核心洞察**:
```
高频用户(≥50券)    核销率 66.37% ⭐⭐⭐
中频用户(20-49券)  核销率 35.74% ⭐⭐
低频用户(10-19券)  核销率 13.57% ⭐
偶尔用户(1-9券)    核销率 6.24% (羊毛党)
```

## 📈 数据可视化

所有可视化图表位于 `output/` 目录：

1. **task1_visualization.png** - 商家优惠券使用堆叠柱状图
2. **task2_visualization.png** - 商家距离分布子图
3. **task3_visualization.png** - 使用间隔直方图和箱线图
4. **task4_visualization.png** - 折扣率和用户活跃度对比图

## 🔧 技术亮点

### 1. 兼容性处理
- 修复Python 3.5不支持f-string问题
- 所有格式化使用`.format()`方法

### 2. 数据去重
- Task2使用`set()`实现用户级别去重

### 3. 两阶段MapReduce
- Task4-2实现复杂的用户活跃度分析

### 4. 本地模式运行
- 使用Hadoop本地模式成功处理43MB数据

## 📁 项目结构

```
bigdata-project2/
├── src/              # MapReduce源代码
│   ├── task1/
│   ├── task2/
│   ├── task3/
│   └── task4/
├── output/           # 分析结果
├── logs/             # 日志和临时结果
├── visualize.py      # 可视化脚本
├── test_local.sh     # 本地测试脚本
├── PLAN.md           # 实验计划
├── RESULTS_SUMMARY.md        # 结果汇总
├── PROJECT_COMPLETION.md     # 完成报告
└── 实验报告.md        # 详细实验报告
```

## 💡 商业洞察

1. **折扣策略**: 低门槛优惠券(95-100%折扣)转化率最高，建议增加此类券的投放
2. **用户运营**: 高频用户价值最大(66.37%核销率)，应重点维护
3. **时效优化**: 用户决策快(中位数4天)，优惠券有效期不宜过长
4. **羊毛党识别**: 偶尔用户(1-9券)核销率仅6.24%，需要精准识别和控制

## 📊 数据规模

- **原始数据**: 43 MB
- **记录数**: ~750,000 条
- **商家数**: 8,018 个
- **优惠券数**: 5,462 个
- **处理模式**: Hadoop本地模式

## 🎓 学习收获

1. 深入理解MapReduce分布式计算原理
2. 掌握Hadoop Streaming与Python集成
3. 学习大数据场景下的数据处理技巧
4. 实践数据可视化和商业分析

## 🚀 改进方向

- [ ] 使用Combiner优化性能
- [ ] 启用YARN分布式资源调度
- [ ] 增加更多分析维度(时间周期、地理位置等)
- [ ] 实现交互式可视化报表
- [ ] 添加单元测试和自动化部署

## 📝 相关文档

- [实验要求](./实验2-MapReduce.md)
- [实验计划](./PLAN.md)
- [实验报告](./实验报告.md)
- [结果汇总](./RESULTS_SUMMARY.md)
- [完成报告](./PROJECT_COMPLETION.md)

## ✅ 项目状态

**所有任务已完成** - 2025年10月27日

---

**作者**: kleene  
**技术栈**: Hadoop 3.4.0 | Python 3 | Docker | matplotlib  
**实验类型**: 大数据MapReduce实验
