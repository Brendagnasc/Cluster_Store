#!/usr/bin/env bash
# Executa toda a suite de testes do TP3 e resume os resultados.
cd "$(dirname "$0")"
chmod +x *.sh
TOTAL_FAIL=0
for t in test1_normal.sh test2_falha_sync.sh test3_omissao.sh \
         test4_concorrencia.sh test5_idempotencia.sh test6_queda_store.sh; do
  echo ""
  ./$t
  TOTAL_FAIL=$((TOTAL_FAIL + $?))
  echo "------------------------------------------------------------"
done
echo ""
if [ $TOTAL_FAIL -eq 0 ]; then
  echo ">>> SUITE COMPLETA: todos os testes passaram."
else
  echo ">>> SUITE COMPLETA: $TOTAL_FAIL verificacao(oes) falharam."
fi
exit $TOTAL_FAIL
