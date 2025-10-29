#!/bin/bash

# Hadoop集群模式WordCount实验脚本
# 运行模式：YARN集群模式（伪分布式）

export HADOOP_HOME=/usr/local/hadoop
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

echo "======================================"
echo "Hadoop集群模式 WordCount 实验"
echo "======================================"
echo ""

# 检查Hadoop服务状态
echo "1. 检查Hadoop服务状态..."
jps
echo ""

# 检查HDFS是否可用
if ! $HADOOP_HOME/bin/hdfs dfs -test -d / 2>/dev/null; then
    echo "错误：HDFS服务未启动！"
    echo "请先启动HDFS服务："
    echo "  $HADOOP_HOME/bin/hdfs --daemon start namenode"
    echo "  $HADOOP_HOME/bin/hdfs --daemon start datanode"
    exit 1
fi

# 清理旧数据
echo "2. 清理旧数据..."
$HADOOP_HOME/bin/hdfs dfs -rm -r /input /output 2>/dev/null
echo ""

# 创建HDFS目录
echo "3. 创建HDFS目录..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /input
echo ""

# 上传数据文件
echo "4. 上传数据文件到HDFS..."
$HADOOP_HOME/bin/hdfs dfs -put /homework/stock_data.csv /input/
echo ""

# 验证文件
echo "5. 验证HDFS文件..."
$HADOOP_HOME/bin/hdfs dfs -ls /input/
echo ""

# 运行WordCount
echo "6. 运行MapReduce WordCount任务..."
echo "   （任务运行中，请访问 http://localhost:8088 查看进度）"
echo ""

$HADOOP_HOME/bin/hadoop jar \
    $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar \
    wordcount \
    /input \
    /output

if [ $? -eq 0 ]; then
    echo ""
    echo "======================================"
    echo "任务完成！查看结果"
    echo "======================================"
    echo ""
    
    # 原始词频统计
    echo "【原始词频统计 Top 30】"
    $HADOOP_HOME/bin/hdfs dfs -cat /output/part-r-00000 | sort -t$'\t' -k2 -nr | head -30
    echo ""
    
    # 过滤停用词后的统计
    echo "======================================"
    echo "【有意义词频统计 Top 50】"
    echo "======================================"
    $HADOOP_HOME/bin/hdfs dfs -cat /output/part-r-00000 | \
        awk -F'\t' 'length($1) > 1' | \
        grep -viE '^(the|to|a|an|and|or|but|in|on|at|for|of|with|from|by|as|is|was|are|be|been|being|have|has|had|do|does|did|will|would|should|could|may|might|can|must|this|that|these|those|it|its|i|you|he|she|we|they|me|him|her|us|them|my|your|his|our|their|what|which|who|when|where|why|how|all|each|every|both|few|more|most|other|some|such|no|nor|not|only|own|same|so|than|too|very|just|up|out|if|about|after|before|into|through|during|above|below|between|under|again|further|then|once|here|there|over|also|any|because|until|while)[[:space:]]' | \
        grep -vE '^(user:|",1|",-1|","|,-1|,1|-|--|---|&|@|#|\$|%|\^|\*|\(|\)|\[|\]|\{|\}|;|:|,|\.|!|\?|<|>|/|\\|\||~|`|'"'"'|")' | \
        grep -vE '^[0-9]+[[:space:]]' | \
        sort -t$'\t' -k2 -nr | head -50
    echo ""
    
    # 股票代码统计
    echo "======================================"
    echo "【股票代码统计 Top 20】"
    echo "======================================"
    $HADOOP_HOME/bin/hdfs dfs -cat /output/part-r-00000 | \
        grep -E '^[A-Z]{2,5}[[:space:]]' | \
        sort -t$'\t' -k2 -nr | head -20
    echo ""
    
    # 统计信息
    echo "======================================"
    echo "【统计信息】"
    echo "======================================"
    total=$($HADOOP_HOME/bin/hdfs dfs -cat /output/part-r-00000 | wc -l)
    filesize=$($HADOOP_HOME/bin/hdfs dfs -du -h /output/part-r-00000 | awk '{print $1}')
    echo "总唯一词数: $total"
    echo "输出文件大小: ${filesize}KB"
    echo ""
    echo "======================================"
    echo "【Web UI 访问地址】"
    echo "======================================"
    echo "YARN资源管理器: http://localhost:8088"
    echo "HDFS浏览器: http://localhost:9870"
    echo "任务历史服务器: http://localhost:19888"
    echo ""
    echo "实验成功完成！"
else
    echo ""
    echo "错误：MapReduce任务执行失败！"
    exit 1
fi
