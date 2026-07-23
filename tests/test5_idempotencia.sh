#!/usr/bin/env bash
# Teste 5: idempotencia por reqid.
# O Sync 0 aplica a escrita no Store e cai ANTES de responder o W3. O cliente
# reenvia o MESMO reqid a outro Sync. O Store deve reconhecer a duplicata e
# NAO aplicar de novo: mesma versao, sem novo incremento.
source "$(dirname "$0")/lib.sh"
echo "== Teste 5: idempotencia por reqid (retentativa nao reaplica) =="
preparar
subir_stores
subir_syncs 0 falhar-antes-w3
rodar_cliente 0 1        # uma unica operacao logica (c0-1)
sleep 2

afirmar "Sync 0 caiu apos escrever e antes do W3 (falha programada)" \
  bash -c "grep -q 'ANTES do W3' '$LOGS/sync0.log'"
afirmar "cliente reenviou o mesmo reqid a outro Sync (falha mascarada)" \
  bash -c "grep -q 'FALHA MASCARADA' '$LOGS/client0.log' && grep -q 'W3 recebido' '$LOGS/client0.log'"
afirmar "Store reconheceu a retentativa como duplicata (reqid ja aplicado)" \
  bash -c "grep -q 'ja aplicada' '$LOGS'/store*.log"
afirmar "a versao final e v1 (a escrita NAO foi aplicada duas vezes)" \
  bash -c "grep -q 'W3 recebido: escrita de R. concluida (v1)' '$LOGS/client0.log'"
afirmar_saida "replicas identicas ao final" verificar_replicas_iguais

limpar_processos
echo "Resultado: $PASSOU pass, $FALHOU fail"
exit $FALHOU
