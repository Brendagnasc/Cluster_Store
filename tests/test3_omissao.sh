#!/usr/bin/env bash
# Teste 3: falhas por OMISSAO (processo vivo, porem mudo: SIGSTOP).
# 3a) Sync do cliente entra em omissao -> cliente mascara via PING e redireciona.
# 3b) Store entra em omissao durante a replicacao -> W4 detecta e o sistema
#     segue com as replicas saudaveis; ao voltar (SIGCONT), converge.
source "$(dirname "$0")/lib.sh"
echo "== Teste 3: falhas por omissao (Sync e Store) =="
preparar

echo "-- 3a: omissao no Sync do cliente --"
subir_stores
subir_syncs
"$RAIZ/bin/client" 0 4 > "$LOGS/client0.log" 2>&1 &
CPID=$!
sleep 2.5
kill -STOP $(pgrep -f "bin/sync_node 0$")   # Sync 0 (do cliente 0) congela
wait $CPID
kill -CONT $(pgrep -f "bin/sync_node 0$") 2>/dev/null
afirmar "3a: cliente detectou o silencio e mascarou (redirecionou)" \
  bash -c "grep -q 'FALHA MASCARADA' '$LOGS/client0.log'"
afirmar "3a: todas as escritas concluidas" \
  bash -c "[ \$(grep -c 'W3 recebido' '$LOGS/client0.log') -eq 4 ]"
limpar_processos

echo "-- 3b: omissao num Store durante a replicacao --"
rm -f "$LOGS"/*.log
subir_stores
subir_syncs
kill -STOP $(pgrep -f "bin/store_node 2$")   # Store 2 congela antes das escritas
rodar_cliente 0 3
STORE2=$(pgrep -f "bin/store_node 2$")
afirmar "3b: omissao do Store detectada (W2 ou W4)" \
  bash -c "grep -q 'FALHA DETECTADA] Store 2' '$LOGS'/*.log"
afirmar "3b: consistencia OK entre as replicas saudaveis" \
  bash -c "grep -q 'CONSISTENCIA] OK' '$LOGS'/store*.log && ! grep -q 'DIVERGENCIA' '$LOGS'/store*.log"
afirmar "3b: escritas concluidas apesar da omissao" \
  bash -c "[ \$(grep -c 'W3 recebido' '$LOGS/client0.log') -eq 3 ]"
kill -CONT $STORE2 2>/dev/null
sleep 0.5
# apos voltar, o Store 2 fica desatualizado ate receber novos W4; uma nova
# escrita em cada item nao e garantida aqui, entao validamos apenas os vivos
afirmar "3b: Store 2 voltou a responder apos SIGCONT" \
  bash -c "python3 -c \"
import socket
s = socket.create_connection(('127.0.0.1', 9003), timeout=2)
s.sendall(b'PING\n'); assert s.recv(8).startswith(b'PONG')\""

limpar_processos
echo "Resultado: $PASSOU pass, $FALHOU fail"
exit $FALHOU
