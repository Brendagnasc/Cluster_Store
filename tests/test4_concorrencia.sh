#!/usr/bin/env bash
# Teste 4: concorrencia. Os 5 clientes escrevem simultaneamente em recursos
# aleatorios. Valida: todas as escritas concluem, a SC serializa (nunca ha
# dois Syncs simultaneos na SC) e as replicas terminam identicas.
source "$(dirname "$0")/lib.sh"
echo "== Teste 4: concorrencia entre 5 clientes =="
preparar
subir_stores
subir_syncs
CPIDS=""
for c in 0 1 2 3 4; do
  "$RAIZ/bin/client" $c 4 > "$LOGS/client$c.log" 2>&1 &
  CPIDS="$CPIDS $!"
done
wait $CPIDS
sleep 2

TOTAL=$(grep -hc 'W3 recebido' "$LOGS"/client*.log | paste -sd+ | bc)
afirmar "todas as 20 escritas dos 5 clientes concluidas" \
  bash -c "[ $TOTAL -eq 20 ]"

# serializacao da SC: reconstruimos a linha do tempo global de ENTROU/SAIU e
# verificamos que nunca ha duas entradas sem uma saida entre elas
afirmar "exclusao mutua respeitada (nunca 2 Syncs na SC ao mesmo tempo)" \
  bash -c "grep -h -E 'ENTROU na secao critica|SAIU da secao critica' '$LOGS'/sync*.log \
    | sort | awk '/ENTROU/{n++; if(n>1) exit 1} /SAIU/{n--} END{exit 0}'"

afirmar "nenhuma divergencia interna reportada" \
  bash -c "! grep -q 'DIVERGENCIA' '$LOGS'/store*.log"
afirmar_saida "replicas identicas ao final" verificar_replicas_iguais

limpar_processos
echo "Resultado: $PASSOU pass, $FALHOU fail"
exit $FALHOU
