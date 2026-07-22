# TP3 (BCC362, Sistemas Distribuídos): Replicação e Tolerância a Falhas

Sistema de replicação com primário dinâmico (Protocolo 2) e tolerância a falhas por queda ou omissão, implementado em C++17 com sockets POSIX e threads, sem middlewares.

## Arquitetura

| Entidade | Quantidade | Portas | Papel |
|---|---|---|---|
| Cliente | 5 | (não escuta) | Escolhe randomicamente um recurso (R0 a R4) para escrever. Conhece apenas um nó Sync por vez. |
| Cluster Sync | 5 | 8001 a 8005 | Recebe as requisições dos clientes (W1) e encaminha a escrita ao Store escolhido, que pode variar a cada entrada na seção crítica. |
| Cluster Store | 3 | 9001 a 9003 | Réplicas. Todos possuem cópia de todos os recursos (nível de replicação 3). |

## Protocolo 2 (primário dinâmico)

A cópia primária de cada item migra para o Store que o Sync deseja atualizar. O fluxo é comprovado nos logs do terminal:

1. **W1**: cliente envia a requisição de escrita ao seu Sync.
2. **W2**: o Store escolhido puxa a cópia primária do item do primário antigo e assume a primária, avisando as demais réplicas.
3. **W3**: o novo primário aplica a escrita localmente e o reconhecimento de escrita concluída volta ao cliente (protocolo não bloqueador: o cliente é liberado antes da propagação).
4. **W4**: o primário diz aos backups para atualizar.
5. **W5**: cada backup reconhece a atualização.

Após W5, o primário executa um **teste de consistência**: lê o item em todas as réplicas saudáveis e confirma que valor e versão são idênticos, registrando `[CONSISTENCIA] OK` no log.

## Tolerância a falhas (queda e omissão)

* **Detecção**: temporizadores (timeout de 1,5 s nas comunicações) e mensagens periódicas de controle PING enviadas pelo cliente ao seu Sync antes de cada operação.
* **Falha de elemento do Cluster Sync (cenário exigido)**: se o Sync do cliente cai (queda) ou congela (omissão), o cliente detecta o silêncio e é redirecionado de forma transparente para outro Sync, reenviando a mesma requisição. O log mostra `[FALHA MASCARADA]` e a operação conclui normalmente, sem o usuário perceber. Cobre os três subcasos: Sync ocioso, Sync com pedido pendente e Sync no meio da operação.
* **Falha de elemento do Cluster Store**: se um Store não responde ao W2 ou ao W4, o log mostra `[FALHA DETECTADA]` e o sistema segue com as réplicas saudáveis; o Sync também tenta outro Store se o escolhido estiver fora.

## Estrutura do projeto

```
tp3-cluster-store/
├── Makefile
├── README.md
├── .gitignore
├── include/
│   └── common.hpp        (portas, logging, sockets com timeout)
├── src/
│   ├── client.cpp        (Cliente: W1, PING, failover transparente)
│   ├── sync_node.cpp     (Cluster Sync: W1 -> Store, W3 ao cliente)
│   └── store_node.cpp    (Cluster Store: W2, W4, W5, consistência)
├── monitor/
│   ├── monitor.py        (servidor do painel: UDP -> SSE, só stdlib do Python 3)
│   └── dashboard.html    (painel no navegador com os eventos reais)
├── scripts/
│   ├── run_all.sh        (sobe tudo, incluindo o monitor, com logs em logs/)
│   ├── kill_node.sh      (simula queda ou omissão de um nó)
│   └── stop_all.sh       (encerra todos os processos)
└── logs/                 (gerado em execução, ignorado pelo git)
```

## Monitor web (painel ao vivo)

Cada `logmsg` dos nós C++ envia uma cópia da linha por UDP (porta 7000) ao monitor, um servidor Python de biblioteca padrão que retransmite os eventos ao navegador via Server-Sent Events. O envio é fire and forget: se o monitor não estiver rodando, o sistema funciona normalmente e nada é perdido no protocolo.

O painel em `http://localhost:8080` mostra: a topologia com os pacotes W1 a W5 animados conforme os eventos reais chegam, a tabela de réplicas (valor@versão por Store, com o primário de cada item migrando ao vivo), o terminal unificado e os contadores de escritas concluídas e falhas mascaradas. Quando um nó cai ou entra em omissão, ele é marcado como SEM RESPOSTA assim que a falha é detectada nos logs, e volta a ativo quando emite eventos de novo.

## Compilação e execução

Requisitos: Linux (ou WSL), `g++` com C++17 e `make`.

```bash
# 1. Compilar
make

# 2. Subir o sistema completo (monitor, 3 Stores, 5 Syncs, 5 Clientes)
./scripts/run_all.sh

# 3. Abrir o painel ao vivo no navegador
#    http://localhost:8080
#    (ou acompanhar pelo terminal: tail -f logs/*.log)

# 4. Simular a queda do Sync 0 (cenário exigido)
./scripts/kill_node.sh sync 0

# 5. Simular omissão (nó vivo porém mudo) no Store 1
./scripts/kill_node.sh store 1 omissao
# para reativar depois: kill -CONT <pid mostrado>

# 6. Encerrar tudo
./scripts/stop_all.sh
```

Execução manual (um terminal por processo), se preferir demonstrar ao vivo:

```bash
./bin/store_node 0     # idem para 1 e 2
./bin/sync_node 0      # idem para 1 a 4
./bin/client 0 10      # cliente 0, 10 operações
```

## O que observar nos logs

* `logs/clientN.log`: W1 enviado, W3 recebido e as linhas `[FALHA MASCARADA]` quando o Sync cai no meio da operação.
* `logs/syncN.log`: W1 recebido, encaminhamento ao Store e W3 devolvido.
* `logs/storeN.log`: W2 (migração da primária), aplicação local da escrita, W4, W5, `[FALHA DETECTADA]` e `[CONSISTENCIA] OK`.
