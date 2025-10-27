#!/bin/bash

echo "======================================"
echo "Homework 5 - Hadoop WordCount"
echo "======================================"

cd /homework
rm -rf output input_temp
mkdir -p input_temp
cp stock_data.csv input_temp/

echo ""
echo "Running Hadoop WordCount..."
/usr/local/hadoop/bin/hadoop jar \
    /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar \
    wordcount \
    -Dmapreduce.framework.name=local \
    -Dfs.defaultFS=file:/// \
    file:///homework/input_temp \
    file:///homework/output

if [ $? -eq 0 ]; then
    echo ""
    echo "======================================"
    echo "原始词频统计 (Top 30):"
    echo "======================================"
    cat output/part-r-00000 | sort -t$'\t' -k2 -nr | head -30
    
    echo ""
    echo "======================================"
    echo "过滤停用词后的有意义词频统计 (Top 50):"
    echo "======================================"
    
    # 过滤停用词、标点符号、情感标签、单字符等
    cat output/part-r-00000 | \
    awk -F'\t' 'length($1) > 1' | \
    grep -viE '^(the|to|a|an|and|or|but|in|on|at|for|of|with|from|by|as|is|was|are|be|been|being|have|has|had|do|does|did|will|would|should|could|may|might|can|must|this|that|these|those|it|its|i|you|he|she|we|they|me|him|her|us|them|my|your|his|our|their|what|which|who|when|where|why|how|all|each|every|both|few|more|most|other|some|such|no|nor|not|only|own|same|so|than|too|very|just|up|out|if|about|after|before|into|through|during|above|below|between|under|again|further|then|once|here|there|over|also|any|because|until|while)[[:space:]]' | \
    grep -vE '^(user:|",1|",-1|","|,-1|,1|-|--|---|&|@|#|\$|%|\^|\*|\(|\)|\[|\]|\{|\}|;|:|,|\.|!|\?|<|>|/|\\|\||~|`|'"'"'|")' | \
    grep -vE '^[0-9]+[[:space:]]' | \
    sort -t$'\t' -k2 -nr | head -50
    
    echo ""
    echo "======================================"
    echo "股票代码词频统计 (Top 20):"
    echo "======================================"
    # 提取2-5个大写字母的股票代码
    cat output/part-r-00000 | \
    grep -E '^[A-Z]{2,5}[[:space:]]' | \
    sort -t$'\t' -k2 -nr | head -20
    
    echo ""
    echo "======================================"
    echo "统计信息:"
    echo "======================================"
    total=$(wc -l < output/part-r-00000)
    filtered=$(cat output/part-r-00000 | \
        awk -F'\t' 'length($1) > 1' | \
        grep -viE '^(the|to|a|an|and|or|but|in|on|at|for|of|with|from|by|as|is|was|are|be|been|being|have|has|had|do|does|did|will|would|should|could|may|might|can|must|this|that|these|those|it|its|i|you|he|she|we|they|me|him|her|us|them|my|your|his|our|their|what|which|who|when|where|why|how|all|each|every|both|few|more|most|other|some|such|no|nor|not|only|own|same|so|than|too|very|just|up|out|if|about|after|before|into|through|during|above|below|between|under|again|further|then|once|here|there|over|also|any|because|until|while)[[:space:]]' | \
        grep -vE '^(user:|",1|",-1|","|,-1|,1|-|--|---|&|@|#|\$|%|\^|\*|\(|\)|\[|\]|\{|\}|;|:|,|\.|!|\?|<|>|/|\\|\||~|`|'"'"'|")' | \
        grep -vE '^[0-9]+[[:space:]]' | wc -l)
    
    echo "总唯一词数: $total"
    echo "有意义词数: $filtered"
    echo "过滤率: $(awk "BEGIN {printf \"%.1f%%\", (1-$filtered/$total)*100}")"
    
    echo ""
    echo "SUCCESS! 结果已保存到 /homework/output/"
fi
