#!/usr/bin/env bash
# Teste 1: execucao normal.
# Sobe o cluster completo, um cliente faz escritas e valida: fluxo W1..W5 nos
# logs, entrada/saida da secao critica, consistencia das replicas.
source "$(dirname "$0")/lib.sh"
echo "== Teste 1: execucao normal (W1..W5, SC, consistencia) =="
preparar
subir_stores
subir_syncs
rodar_cliente 0 4
sleep 2

afirmar "cliente concluiu as 4 escritas (W3 recebido 4x)" \
  bash -c "[ \$(grep -c 'W3 recebido' '$LOGS/client0.log') -eq 4 ]"
afirmar "Sync entrou e saiu da secao critica a cada escrita" \
  bash -c "[ \$(grep -hc 'ENTROU na secao critica' '$LOGS'/sync*.log | paste -sd+ | bc) -ge 4 ] && \
           [ \$(grep -hc 'SAIU da secao critica' '$LOGS'/sync*.log | paste -sd+ | bc) -ge 4 ]"
afirmar "fluxo completo registrado (W1, W2 ou primaria local, W4, W5)" \
  bash -c "grep -q 'W1: requisicao' '$LOGS'/sync*.log && grep -q 'W4: dizendo' '$LOGS'/store*.log && grep -q 'W5:' '$LOGS'/store*.log"
afirmar "teste de consistencia interno reportou OK" \
  bash -c "grep -q 'CONSISTENCIA] OK' '$LOGS'/store*.log"
afirmar "nenhuma divergencia interna reportada" \
  bash -c "! grep -q 'DIVERGENCIA' '$LOGS'/store*.log"
afirmar_saida "replicas identicas ao final (leitura externa)" verificar_replicas_iguais

limpar_processos
echo "Resultado: $PASSOU pass, $FALHOU fail"
exit $FALHOU
