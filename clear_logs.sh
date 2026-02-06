#!/bin/bash
# 清空 logs 目录下所有日志文件（保留文件，清空内容）
LOG_DIR="$(dirname "$0")/logs"

if [ ! -d "$LOG_DIR" ]; then
    echo "logs 目录不存在: $LOG_DIR"
    exit 1
fi

count=0
for f in "$LOG_DIR"/*.log; do
    [ -f "$f" ] || continue
    > "$f"
    count=$((count + 1))
done

echo "已清空 $count 个日志文件"
