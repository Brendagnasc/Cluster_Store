#!/usr/bin/env bash
# Sobe o sistema completo: 3 Stores, 5 Syncs e 5 Clientes.
# Cada processo grava seu log em logs/. Acompanhe com: tail -f logs/*.log
set -e
cd "$(dirname "$0")/.."
make -s
mkdir -p logs
rm -f logs/*.log logs/pids.txt

echo "Subindo monitor web em http://localhost:8080 ..."
python3 monitor/monitor.py > logs/monitor.log 2>&1 &
echo $! >> logs/pids.txt
sleep 0.5

echo "Subindo Cluster Store (3 elementos)..."
for i in 0 1 2; do
  ./bin/store_node $i > logs/store$i.log 2>&1 &
  echo $! >> logs/pids.txt
done
sleep 1

echo "Subindo Cluster Sync (5 elementos)..."
for i in 0 1 2 3 4; do
  ./bin/sync_node $i > logs/sync$i.log 2>&1 &
  echo $! >> logs/pids.txt
done
sleep 1

echo "Subindo 5 clientes (operando continuamente ate voce parar com stop_all.sh)..."
for i in 0 1 2 3 4; do
  ./bin/client $i > logs/client$i.log 2>&1 &
  echo $! >> logs/pids.txt
done

echo ""
echo "Sistema no ar. PIDs em logs/pids.txt"
echo "Painel ao vivo:         http://localhost:8080"
echo "Acompanhe os logs com:  tail -f logs/*.log"
echo "Derrube um Sync com:    ./scripts/kill_node.sh sync 0"
echo "Encerre tudo com:       ./scripts/stop_all.sh"
