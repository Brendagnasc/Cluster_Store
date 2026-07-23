#!/usr/bin/env bash
# Teste 6: queda de um elemento do Cluster Store detendo a primaria.
# O Store primario de um item cai; a proxima escrita migra a primaria de forma
# SEGURA (sem autopromocao cega por timeout: adota a versao mais recente entre
# as replicas vivas, sob a serializacao da SC). Depois o Store religa e
# recupera o estado junto ao cluster.
source "$(dirname "$0")/lib.sh"
echo "== Teste 6: queda de Store primario e migracao segura =="
preparar
subir_stores
subir_syncs
rodar_cliente 0 3          # gera escritas e espalha primarias
kill -9 $(pgrep -f "bin/store_node 0$")
rodar_cliente 1 3          # novas escritas com o Store 0 morto
sleep 2

afirmar "queda do Store 0 detectada (W2 ou W4 ou encaminhamento)" \
  bash -c "grep -q 'FALHA DETECTADA] Store 0' '$LOGS'/*.log"
afirmar "escritas seguiram concluindo com 2 replicas" \
  bash -c "[ \$(grep -c 'W3 recebido' '$LOGS/client1.log') -eq 3 ]"
afirmar "nenhuma divergencia entre as replicas vivas" \
  bash -c "! grep -q 'DIVERGENCIA' '$LOGS'/store*.log"

# religa o Store 0: deve recuperar estado e primarias junto ao cluster
"$RAIZ/bin/store_node" 0 >> "$LOGS/store0.log" 2>&1 &
sleep 1.5
afirmar "Store 0 religado recuperou o estado junto ao cluster" \
  bash -c "grep -q 'Estado recuperado' '$LOGS/store0.log'"

limpar_processos
echo "Resultado: $PASSOU pass, $FALHOU fail"
exit $FALHOU
