#!/usr/bin/env bash
# Encerra todos os processos do sistema.
cd "$(dirname "$0")/.."
pkill -9 -f "bin/store_node" 2>/dev/null || true
pkill -9 -f "bin/sync_node"  2>/dev/null || true
pkill -9 -f "bin/client"     2>/dev/null || true
pkill -9 -f "monitor/monitor.py" 2>/dev/null || true
rm -f logs/pids.txt
echo "Todos os processos encerrados."
