#!/bin/bash
# 运行所有Hadoop任务

HADOOP_HOME="/usr/local/hadoop"
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar"

run_task() {
    local TASK_NAME=$1
    local TASK_NUM=$2
    local MAPPER=$3
    local REDUCER=$4
    local INPUT=${5:-"/input/ccf_offline_stage1_train.csv"}
    
    echo "==========================================="
    echo "$TASK_NAME"
    echo "==========================================="
    
    # 删除旧输出
    $HADOOP_HOME/bin/hdfs dfs -rm -r /output/task$TASK_NUM 2>/dev/null
    
    # 运行MapReduce
    $HADOOP_HOME/bin/hadoop jar $STREAMING_JAR \
        -D mapreduce.framework.name=local \
        -D mapreduce.job.maps=1 \
        -D mapreduce.job.reduces=1 \
        -files $MAPPER,$REDUCER \
        -input $INPUT \
        -output /output/task$TASK_NUM \
        -mapper "python3 $(basename $MAPPER)" \
        -reducer "python3 $(basename $REDUCER)" 2>&1 | grep -E "(Job |map |reduce |成功|完成)" | tail -5
    
    echo "✓ 任务完成，下载结果..."
    $HADOOP_HOME/bin/hdfs dfs -get /output/task$TASK_NUM/part-00000 /workspace/output/task$TASK_NUM/result.txt 2>/dev/null
    echo ""
}

# 任务二
run_task "任务二：商家距离统计" "2" \
    "/workspace/src/task2/mapper.py" \
    "/workspace/src/task2/reducer.py"

# 任务三
run_task "任务三：优惠券使用间隔统计" "3" \
    "/workspace/src/task3/mapper2.py" \
    "/workspace/src/task3/reducer2.py"

# 任务四-折扣率
run_task "任务四-1：折扣率影响分析" "4_discount" \
    "/workspace/src/task4/mapper_discount.py" \
    "/workspace/src/task4/reducer_discount.py"

# 任务四-用户活跃度 (阶段1)
run_task "任务四-2a：用户活跃度分析(阶段1)" "4_user_stage1" \
    "/workspace/src/task4/mapper_user_activity.py" \
    "/workspace/src/task4/reducer_user_activity1.py"

# 任务四-用户活跃度 (阶段2)
run_task "任务四-2b：用户活跃度分析(阶段2)" "4_user_stage2" \
    "/workspace/src/task4/mapper_user_activity2.py" \
    "/workspace/src/task4/reducer_user_activity2.py" \
    "/output/task4_user_stage1"

echo "==========================================="
echo "所有任务完成！"
echo "==========================================="
