```markdown
# 实验3：基于Hive的优惠券使用数据分析

---

## 任务一：导入数据

### 描述
1. 创建两张内部表 `ccf_offline_stage1_train` 和 `ccf_online_stage1_train`，字段名和数据类型与 CSV 文件一一对应。
2. 使用 `LOAD DATA` 或 `INSERT INTO` 命令将 CSV 文件加载到对应 Hive 表中。加载完成后，使用 `SELECT * LIMIT 5` 查询验证数据加载成功。

### 输出要求
在报告中提供以下 Hive 命令及其执行结果截图：

```sql
SELECT * FROM ccf_offline_stage1_train LIMIT 5;
SELECT * FROM ccf_online_stage1_train LIMIT 5;

SELECT COUNT(*) FROM ccf_offline_stage1_train;
SELECT COUNT(*) FROM ccf_online_stage1_train;
```

---

## 任务二：基本数据查询

### 1. 查询用户行为数量
使用 `ccf_online_stage1_train` 统计三种行为（点击、购买、领取）的总次数，并按数量降序排列输出。

#### 输出格式：
```
<行为> <总次数>
```

---

### 2. 查询指定商家优惠券使用情况
使用 `ccf_online_stage1_train` 统计每个商家的优惠券使用情况，分为负样本、普通消费和正样本三种，并将结果存储在新表 `online_consumption_table` 中。

> 注：
> - 如果 `Date=null` 且 `Coupon_id != null`，表示领取优惠券但没有使用，即负样本；
> - 如果 `Date!=null` 且 `Coupon_id = null`，表示普通消费；
> - 如果 `Date!=null` 且 `Coupon_id != null`，表示用优惠券消费，即正样本。

#### 输出要求：
```sql
SELECT * FROM online_consumption_table LIMIT 20;
```

#### 输出格式：
```
<Merchant_id> <负样本数量> <普通消费数量> <正样本数量>
```

---

## 任务三：数据聚合分析

### 1. 商家周边活跃顾客数量统计
根据 `ccf_offline_stage1_train` 表中数据，对每个商家与周边消费者的距离进行统计，给出不同距离的活跃消费者人数。注意表中 `Distance` 字段缺失为 `NULL`。

#### 输出格式：
```
<Merchant_id> TAB <距离为x的消费者人数>
```

---

### 2. 商家正样本比例统计
根据 `online_consumption_table` 表中数据，按正样本比例对商家排序，给出正样本比例最高的前十个商家。

#### 输出格式：
```
<Merchant_id> <正样本比例> <正样本数量> <总样本数量>
```

---

## 任务四：复杂查询与分析

### 1. 优惠券使用时间统计
根据 `ccf_offline_stage1_train` 表中数据，统计每一种优惠券的被使用次数（`received && consumed`），`Coupon_id` 缺失项不计入总使用次数。对于被使用次数大于总使用次数 1% 的优惠券，按使用次数排序并给出它们从领取到被使用的平均间隔。

#### 输出格式：
```
<Coupon_id> <使用次数> <平均消费间隔>
```

---

### 2. 优惠券折扣率统计
根据 `ccf_offline_stage1_train` 表中数据，统计每一种优惠券的被使用率（使用次数 / 总消费次数），`Coupon_id` 缺失项不计入总使用次数。对于被使用率前十的优惠券，按使用率排序并给出它们的使用次数和折扣率，折扣率以百分比形式给出。

> 注：折扣率 = (原价 - 折后价) / 原价 × 100%

#### 输出格式：
```
<Coupon_id> <使用率> <使用次数> <折扣率> <折扣方式>
```

---

## 实验报告要求

- 在实验报告中给出解答每一个问题的 Hive QL 代码并给出对应的结果截图。
- 可写本次实验的感受以及收获。
- 如有 Hive 优化或对优惠券使用预测任务的想法，也可一并呈现。

> 注意：
> 1. 本实验对输出格式无具体要求，主要考察 Hive 的查询结果。也即，无需对输出结果中的间隔符进行额外修改。并且，在报告中呈现的结果请使⽤**截图**显示。
> 2. 报告需要包含一个清晰的大纲结构（Markdown 中的多级标题或是 Word 的导航窗格），避免大段截图堆砌和日志内容，嵌入图片后导出为 PDF 再上传到课程网站（请尽量不要超过 20 页）。
```