#!/bin/bash

# Run all tasks
# Ensure Spark environment is set up before running this script.

echo "========================================"
echo "Running Task 1: Spark RDD Programming"
echo "========================================"
python3 src/task1_rdd.py

echo ""
echo "========================================"
echo "Running Task 2: Spark SQL Programming"
echo "========================================"
python3 src/task2_sql.py

echo ""
echo "========================================"
echo "Running Task 3: Spark MLlib Programming"
echo "========================================"
python3 src/task3_mllib.py
