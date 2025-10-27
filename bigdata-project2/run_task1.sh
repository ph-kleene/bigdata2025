#!/bin/bash
# 单个任务运行脚本 - 任务一

HADOOP_HOME="/usr/local/hadoop"
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar"

echo "==========================================="
echo "任务一：商家优惠券使用情况统计"
echo "==========================================="

# 删除旧的输出目录
$HADOOP_HOME/bin/hdfs dfs -rm -r /output/task1 2>/dev/null

# 运行MapReduce任务 (本地模式)
$HADOOP_HOME/bin/hadoop jar $STREAMING_JAR \
    -D mapreduce.framework.name=local \
    -D mapreduce.job.maps=1 \
    -D mapreduce.job.reduces=1 \
    -files /workspace/src/task1/mapper.py,/workspace/src/task1/reducer.py \
    -input /input/ccf_offline_stage1_train.csv \
    -output /output/task1 \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py"

echo ""
echo "任务完成！查看结果："
$HADOOP_HOME/bin/hdfs dfs -cat /output/task1/part-* | head -20
