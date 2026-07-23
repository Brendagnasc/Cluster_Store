#!/usr/bin/env bash
# Teste 2: Opcao 1 do enunciado, falha de elemento do Cluster Sync nos 3 estados:
#   1.1 Sync ocioso (sem pedido de cliente)  -> kill -9 num Sync sem clientes
#   1.2 Sync com pedido do cliente           -> flag falhar-apos-w1
#   1.3 Sync dentro da secao critica         -> flag falhar-na-sc
# Em todos: o cliente nao pode perceber (falha mascarada) e as escritas concluem.
source "$(dirname "$0")/lib.sh"
echo "== Teste 2: Opcao 1, falha do Sync nos 3 estados =="
preparar

echo "-- caso 1.1: Sync ocioso cai --"
subir_stores
subir_syncs
"$RAIZ/bin/client" 0 3 > "$LOGS/client0.log" 2>&1 &
CPID=$!
sleep 2.5
kill -9 $(pgrep -f "bin/sync_node 3$")   # Sync 3 esta ocioso (cliente 0 usa Sync 0)
wait $CPID
afirmar "1.1: escritas concluidas com Sync ocioso morto" \
  bash -c "[ \$(grep -c 'W3 recebido' '$LOGS/client0.log') -eq 3 ]"
afirmar "1.1: SC seguiu sem deadlock apos remover o Sync morto do conjunto ativo" \
  bash -c "[ \$(grep -c 'W3 recebido' '$LOGS/client0.log') -eq 3 ]"
limpar_processos

echo "-- caso 1.2: Sync cai apos receber o W1 --"
rm -f "$LOGS"/*.log
subir_stores
subir_syncs 0 falhar-apos-w1          # Sync 0 cai ao receber o primeiro W1
rodar_cliente 0 3                      # cliente 0 comeca no Sync 0
afirmar "1.2: Sync caiu apos o W1 (falha programada disparou)" \
  bash -c "grep -q 'caindo APOS receber o W1' '$LOGS/sync0.log'"
afirmar "1.2: cliente mascarou a falha e reenviou a outro Sync" \
  bash -c "grep -q 'FALHA MASCARADA' '$LOGS/client0.log'"
afirmar "1.2: todas as escritas concluidas mesmo assim" \
  bash -c "[ \$(grep -c 'W3 recebido' '$LOGS/client0.log') -eq 3 ]"
afirmar_saida "1.2: replicas consistentes ao final" verificar_replicas_iguais
limpar_processos

echo "-- caso 1.3: Sync cai DENTRO da secao critica --"
rm -f "$LOGS"/*.log
subir_stores
subir_syncs 0 falhar-na-sc            # Sync 0 cai dentro da SC
rodar_cliente 0 3
afirmar "1.3: Sync caiu dentro da SC (falha programada disparou)" \
  bash -c "grep -q 'caindo DENTRO da secao critica' '$LOGS/sync0.log'"
afirmar "1.3: cliente mascarou a falha (nao percebeu)" \
  bash -c "grep -q 'FALHA MASCARADA' '$LOGS/client0.log'"
afirmar "1.3: SC foi recuperada, demais Syncs seguiram escrevendo (sem deadlock)" \
  bash -c "[ \$(grep -c 'W3 recebido' '$LOGS/client0.log') -eq 3 ]"
afirmar_saida "1.3: replicas consistentes ao final" verificar_replicas_iguais

limpar_processos
echo "Resultado: $PASSOU pass, $FALHOU fail"
exit $FALHOU
