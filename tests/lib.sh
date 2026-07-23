#!/usr/bin/env bash
# tests/lib.sh
# Funcoes compartilhadas pelos testes automatizados. Todos os testes rodam no
# modo local (processos), o mesmo codigo que roda nos containers.
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="$RAIZ/tests/logs"

limpar_processos() {
  pkill -9 -f "bin/store_node" 2>/dev/null
  pkill -9 -f "bin/sync_node"  2>/dev/null
  pkill -9 -f "bin/client"     2>/dev/null
  sleep 0.3
}

subir_stores() {
  for i in 0 1 2; do "$RAIZ/bin/store_node" $i > "$LOGS/store$i.log" 2>&1 & done
  sleep 0.6
}

# subir_syncs [id_com_falha] [flag_de_falha]
subir_syncs() {
  local id_falha="${1:--1}" flag="${2:-}"
  for i in 0 1 2 3 4; do
    if [ "$i" = "$id_falha" ]; then
      "$RAIZ/bin/sync_node" $i "$flag" > "$LOGS/sync$i.log" 2>&1 &
    else
      "$RAIZ/bin/sync_node" $i > "$LOGS/sync$i.log" 2>&1 &
    fi
  done
  sleep 0.6
}

# roda um cliente com N operacoes e espera terminar (o warmup interno e de 2s)
rodar_cliente() {
  local id="$1" ops="$2"
  "$RAIZ/bin/client" "$id" "$ops" > "$LOGS/client$id.log" 2>&1
}

# le item de um store: imprime "valor versao"
ler_store() {
  local store="$1" item="$2"
  python3 - "$store" "$item" << 'PYEOF'
import socket, sys
store, item = int(sys.argv[1]), sys.argv[2]
try:
    s = socket.create_connection(("127.0.0.1", 9001 + store), timeout=2)
    s.settimeout(2)
    s.sendall(f"READ|{item}\n".encode())
    resp = s.recv(256).decode().strip().split("|")
    # VALUE|item|valor|versao|primario|reqid
    print(resp[2], resp[3])
except OSError:
    print("SEM_RESPOSTA -1")
PYEOF
}

# verifica que todas as replicas VIVAS tem o mesmo (valor, versao) para cada item
verificar_replicas_iguais() {
  local ok=0
  for i in 0 1 2 3 4; do
    local item="R$i" ref=""
    for s in 0 1 2; do
      local lido; lido=$(ler_store $s "$item")
      [ "$lido" = "SEM_RESPOSTA -1" ] && continue
      if [ -z "$ref" ]; then ref="$lido";
      elif [ "$lido" != "$ref" ]; then
        echo "  DIVERGENCIA em $item: Store $s tem '$lido', esperado '$ref'"
        ok=1
      fi
    done
  done
  return $ok
}

PASSOU=0; FALHOU=0
afirmar() {  # afirmar "descricao" comando...
  local desc="$1"; shift
  if "$@" > /dev/null 2>&1; then
    echo "  [PASS] $desc"; PASSOU=$((PASSOU+1))
  else
    echo "  [FAIL] $desc"; FALHOU=$((FALHOU+1))
  fi
}
afirmar_saida() {  # afirmar_saida "descricao" (funcao que ecoa problemas e retorna 0/1)
  local desc="$1"; shift
  local saida; saida=$("$@" 2>&1); local rc=$?
  if [ $rc -eq 0 ]; then
    echo "  [PASS] $desc"; PASSOU=$((PASSOU+1))
  else
    echo "  [FAIL] $desc"; [ -n "$saida" ] && echo "$saida"; FALHOU=$((FALHOU+1))
  fi
}

preparar() {
  mkdir -p "$LOGS"; rm -f "$LOGS"/*.log
  limpar_processos
  cd "$RAIZ" && make -s
}
