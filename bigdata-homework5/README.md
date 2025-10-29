# Hadoop MapReduce 词频统计实验报告

## 一、实验环境

- **操作系统**: Linux (Ubuntu/Debian)
- **Hadoop版本**: 3.4.0
- **运行模式**: 伪分布式模式 (Pseudo-Distributed Mode)
- **Docker镜像**: newhadoop
- **数据文件**: stock_data.csv (472KB, 6,090行股票评论)

## 二、实验步骤

### 2.1 创建Docker容器

```bash
docker run -itd --name hadoop-homework5 \
  -p 9870:9870 \
  -p 8088:8088 \
  -p 19888:19888 \
  -v /home/kleene/workspace/bigdata2025/bigdata-homework5:/homework \
  newhadoop /bin/bash
```

**端口说明**：
- 9870: HDFS NameNode Web UI
- 8088: YARN ResourceManager Web UI  
- 19888: MapReduce JobHistory Server

### 2.2 配置Hadoop伪分布式模式

#### 配置core-site.xml
```bash
docker exec hadoop-homework5 bash -c "cat > /usr/local/hadoop/etc/hadoop/core-site.xml << 'EOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop</value>
    </property>
</configuration>
EOF"
```

#### 配置hdfs-site.xml
```bash
docker exec hadoop-homework5 bash -c "cat > /usr/local/hadoop/etc/hadoop/hdfs-site.xml << 'EOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOF"
```

#### 配置yarn-site.xml
```bash
docker exec hadoop-homework5 bash -c "cat > /usr/local/hadoop/etc/hadoop/yarn-site.xml << 'EOF'
<?xml version=\"1.0\"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
EOF"
```

#### 配置mapred-site.xml
```bash
docker exec hadoop-homework5 bash -c "cat > /usr/local/hadoop/etc/hadoop/mapred-site.xml << 'EOF'
<?xml version=\"1.0\"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>localhost:19888</value>
    </property>
</configuration>
EOF"
```

### 2.3 配置SSH免密登录

```bash
docker exec hadoop-homework5 bash -c "
apt-get update && apt-get install -y openssh-server openssh-client
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
service ssh start
"
```

### 2.4 初始化并启动Hadoop

```bash
# 格式化HDFS
docker exec hadoop-homework5 bash -c "
rm -rf /tmp/hadoop
/usr/local/hadoop/bin/hdfs namenode -format -force
"

# 启动Hadoop服务
docker exec hadoop-homework5 bash /homework/start_hadoop.sh
```

### 2.5 运行WordCount任务

```bash
# 上传数据到HDFS
docker exec hadoop-homework5 bash -c "
export HADOOP_HOME=/usr/local/hadoop
\$HADOOP_HOME/bin/hdfs dfs -mkdir -p /input
\$HADOOP_HOME/bin/hdfs dfs -put /homework/stock_data.csv /input/
"

# 执行MapReduce任务
docker exec hadoop-homework5 bash /homework/run_cluster.sh
```

## 三、实验结果

### 3.1 Web UI监控

任务运行时可通过以下Web界面监控：

- **YARN ResourceManager**: http://localhost:8088
  - 查看任务列表和运行状态
  - 监控集群资源使用情况
  
- **HDFS NameNode**: http://localhost:9870
  - 查看HDFS集群状态
  - 浏览文件系统
  
- **JobHistory Server**: http://localhost:19888
  - 查看已完成任务的详细信息
  - 查看性能计数器

**截图说明**：
- 实验截图保存在 `picture/` 目录下
- 截取了以下关键界面：
  - YARN ResourceManager 任务列表页面
  - HDFS NameNode 文件浏览器
  - JobHistory Server 任务详情和计数器
  - MapReduce任务运行过程

### 3.2 任务执行统计

```
Job Counters:
  - Launched map tasks=1
  - Launched reduce tasks=1
  - Total time spent by all maps: 4272ms
  - Total time spent by all reduces: 1365ms

Map-Reduce Framework:
  - Map input records=6090
  - Map output records=82921
  - Reduce input groups=19756
  - Reduce output records=19756

File System Counters:
  - HDFS: Bytes Read=480075
  - HDFS: Bytes Written=202374
```

### 3.3 词频统计结果（Top 30）

```
词语      出现次数
the      1794
to       1668
a        1280
on       1029
of       944
in       886
for      866
and      850
is       811
AAP      765
at       541
this     459
it       443
I        436
up       342
from     331
will     330
be       323
with     322
short    278
that     276
like     267
are      261
user:    255
over     253
out      245
stock    158
buy      142
down     140
watch    134
```

**统计摘要**：
- 总唯一词数: 19,756
- 输入文件: 472 KB
- 输出文件: 197.6 KB
- 处理时间: 约10-18秒

## 四、MapReduce工作原理

### 4.1 WordCount算法流程

#### Map阶段
```
输入: (偏移量, "Kickers on my watchlist XIDE TIT")
处理: 分词 → ["kickers", "on", "my", "watchlist", "xide", "tit"]
输出: 
  (kickers, 1)
  (on, 1)
  (my, 1)
  (watchlist, 1)
  (xide, 1)
  (tit, 1)
```

#### Shuffle & Sort阶段
```
分组排序后:
  (kickers, [1, 1, 1])
  (on, [1, 1, 1, 1, ..., 1])  // 1029个1
  (watchlist, [1])
  ...
```

#### Reduce阶段
```
输入: (on, [1, 1, 1, ..., 1])
处理: sum([1, 1, 1, ..., 1]) = 1029
输出: (on, 1029)
```

### 4.2 Map函数伪代码

```java
public class WordCountMapper {
    public void map(LongWritable key, Text value, Context context) {
        // key: 文件偏移量
        // value: 一行文本
        
        String line = value.toString().toLowerCase();
        String[] words = line.split("\\s+");
        
        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
}
```

### 4.3 Reduce函数伪代码

```java
public class WordCountReducer {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        // key: 单词
        // values: 该单词对应的所有计数值
        
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        context.write(key, new IntWritable(sum));
    }
}
```

### 4.4 Combiner优化

Combiner在Map端进行局部聚合，减少网络传输：

```
Map输出: (the, 1), (the, 1), (the, 1), ..., (the, 1)  // 假设有100个
↓ Combiner局部聚合
Combiner输出: (the, 100)
↓ 网络传输（只传输一条记录）
Reduce输入: (the, [100, 85, 92, ...])  // 来自不同Map任务
```

本实验中Combiner效果：
- Combine input records: 82,921
- Combine output records: 19,756
- **压缩率**: 76.2%

## 五、遇见的问题与解决方案

### 5.1 服务无法启动

**问题**: jps显示某些服务未启动

**解决方案**:
```bash
# 查看日志
docker exec hadoop-homework5 tail -100 /usr/local/hadoop/logs/hadoop-root-namenode-*.log

# 重新格式化（会清空数据）
docker exec hadoop-homework5 bash -c "
rm -rf /tmp/hadoop
/usr/local/hadoop/bin/hdfs namenode -format -force
"
```

### 5.2 Web UI无法访问

**问题**: 浏览器无法打开 http://localhost:8088

**解决方案**:
```bash
# 检查端口映射
docker port hadoop-homework5

# 检查服务是否运行
docker exec hadoop-homework5 jps

# 确认容器创建时添加了-p参数
```

### 5.3 任务执行失败

**问题**: MapReduce任务报错

**解决方案**:
```bash
# 查看任务日志
docker exec hadoop-homework5 bash -c "/usr/local/hadoop/bin/yarn logs -applicationId application_xxx"

# 检查HDFS空间
docker exec hadoop-homework5 bash -c "/usr/local/hadoop/bin/hdfs dfsadmin -report"
```

### 5.4 输出目录已存在

**问题**: `Output directory already exists`

**解决方案**:
```bash
# 删除输出目录
docker exec hadoop-homework5 bash -c "/usr/local/hadoop/bin/hdfs dfs -rm -r /output"
```

## 六、快速参考

### 6.1 一键运行命令

```bash
# 完整流程
docker run -itd --name hadoop-homework5 -p 9870:9870 -p 8088:8088 -p 19888:19888 -v $(pwd):/homework newhadoop /bin/bash
docker exec hadoop-homework5 bash /homework/start_hadoop.sh
docker exec hadoop-homework5 bash /homework/run_cluster.sh
```

### 6.2 常用HDFS命令

```bash
# 查看文件
hdfs dfs -ls /input

# 上传文件
hdfs dfs -put local.txt /input/

# 下载文件
hdfs dfs -get /output/part-r-00000 ./

# 查看文件内容
hdfs dfs -cat /output/part-r-00000 | head

# 删除文件/目录
hdfs dfs -rm -r /output
```

### 6.3 项目文件说明

```
bigdata-homework5/
├── stock_data.csv          # 输入数据（股票评论）
├── start_hadoop.sh         # Hadoop服务启动脚本
├── run_cluster.sh          # WordCount运行脚本（集群模式）
├── run.sh                  # WordCount运行脚本（本地模式）
├── README.md               # 实验报告（本文件）
├── output/                 # MapReduce输出结果
├── input_temp/             # 临时输入目录
└── picture/                # 实验截图
```

---

**实验完成时间**: 2025年10月29日  
**Hadoop版本**: 3.4.0  
**运行模式**: 伪分布式模式 (Pseudo-Distributed Mode)
