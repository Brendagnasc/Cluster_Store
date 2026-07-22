#!/usr/bin/env bash
# Simula falhas em um no do sistema.
#   Queda:   ./scripts/kill_node.sh sync 0          (mata o processo, kill -9)
#   Omissao: ./scripts/kill_node.sh sync 0 omissao  (congela o processo, SIGSTOP:
#            ele continua vivo mas nao responde, caracterizando falha por omissao)
set -e
TIPO=$1   # sync | store
ID=$2     # numero do no
MODO=${3:-queda}

if [ -z "$TIPO" ] || [ -z "$ID" ]; then
  echo "Uso: $0 <sync|store> <id> [queda|omissao]"
  exit 1
fi

PID=$(pgrep -f "bin/${TIPO}_node $ID\$" | head -n1)
if [ -z "$PID" ]; then
  echo "Processo ${TIPO}_node $ID nao encontrado."
  exit 1
fi

if [ "$MODO" = "omissao" ]; then
  kill -STOP "$PID"
  echo "Falha por OMISSAO simulada: ${TIPO} $ID (PID $PID) congelado com SIGSTOP."
  echo "Para reativar: kill -CONT $PID"
else
  kill -9 "$PID"
  echo "Falha por QUEDA simulada: ${TIPO} $ID (PID $PID) morto com kill -9."
fi
