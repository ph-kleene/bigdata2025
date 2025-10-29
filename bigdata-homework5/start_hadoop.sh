#!/bin/bash

# Hadoop服务启动脚本

export HADOOP_HOME=/usr/local/hadoop
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

echo "======================================"
echo "启动 Hadoop 服务"
echo "======================================"
echo ""

# 启动SSH服务
echo "1. 启动SSH服务..."
service ssh start
echo ""

# 启动HDFS
echo "2. 启动HDFS服务..."
$HADOOP_HOME/bin/hdfs --daemon start namenode
echo "   - NameNode 启动中..."
sleep 3

$HADOOP_HOME/bin/hdfs --daemon start datanode
echo "   - DataNode 启动中..."
sleep 3

$HADOOP_HOME/bin/hdfs --daemon start secondarynamenode
echo "   - SecondaryNameNode 启动中..."
sleep 3

# 启动YARN
echo ""
echo "3. 启动YARN服务..."
$HADOOP_HOME/bin/yarn --daemon start resourcemanager
echo "   - ResourceManager 启动中..."
sleep 3

$HADOOP_HOME/bin/yarn --daemon start nodemanager
echo "   - NodeManager 启动中..."
sleep 3

# 启动JobHistory Server
echo ""
echo "4. 启动JobHistory服务..."
$HADOOP_HOME/bin/mapred --daemon start historyserver
echo "   - JobHistoryServer 启动中..."
sleep 3

# 检查服务状态
echo ""
echo "======================================"
echo "Hadoop 服务状态"
echo "======================================"
jps
echo ""

# 显示Web UI地址
echo "======================================"
echo "Web UI 访问地址"
echo "======================================"
echo "NameNode:        http://localhost:9870"
echo "ResourceManager: http://localhost:8088"
echo "JobHistory:      http://localhost:19888"
echo ""
echo "所有服务已启动！"
