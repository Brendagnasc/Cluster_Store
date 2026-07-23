# TP3 (BCC362, Sistemas Distribuídos): Replicação com Primário Móvel e Tolerância a Falhas

Aluna: Brenda (Matrícula 24.1.4011)

Implementação completa do TP3 com o **Protocolo 2 (cópia primária móvel)**, a **exclusão mútua distribuída do TP2 (Ricart-Agrawala)** entre os elementos do Cluster Sync, e tolerância a **falhas por queda e omissão**, em C++17 com sockets POSIX e threads. Sem middleware; todos os nós são processos independentes que se comunicam exclusivamente por sockets TCP (e UDP apenas para o espelhamento de logs ao monitor, que é observacional e não participa do protocolo). A solução não é apenas multicore: os processos podem rodar em máquinas ou containers distintos (modo Docker Compose incluso).

## Arquitetura

| Entidade | Qtde | Portas | Papel |
|---|---|---|---|
| Cliente | 5 | (não escuta) | Escolhe aleatoriamente um recurso (R0 a R4) a cada requisição. Conhece um único Sync por vez; troca de Sync apenas quando o seu falha. |
| Cluster Sync | 5 | 8001 a 8005 | Recebe o W1 do cliente, disputa a seção crítica distribuída e, autorizado, acessa o Cluster Store. |
| Cluster Store | 3 | 9001 a 9003 | Réplicas com cópia de todos os recursos (nível de replicação 3). A cópia primária de cada item migra entre eles (Protocolo 2). |
| Monitor (opcional) | 1 | 8080 (HTTP), 7000 (UDP) | Painel web ao vivo: topologia, mensagens W1 a W5, seção crítica, injeção de falhas reais. Observacional, fora do protocolo. |

## Exclusão mútua distribuída (TP2): Ricart-Agrawala

Um Sync **somente acessa o Cluster Store dentro da seção crítica**. Para entrar:

1. O Sync incrementa seu relógio lógico de Lamport e envia `RA_REQ|ts|id` aos outros 4 Syncs.
2. Cada par responde `RA_REP` imediatamente se não está interessado ou se o pedido recebido tem prioridade (ordem total pelo par `(ts, id)`); caso contrário, **adia** a resposta, mantendo a conexão aberta, e só autoriza ao sair da própria SC.
3. Com as 4 autorizações (explícitas, ou presumidas para pares comprovadamente fora do ar), o Sync entra na SC, executa a escrita no Cluster Store e, ao sair, libera as autorizações adiadas.

Recuperação da SC quando um Sync falha:

* **Par morto (queda)**: a conexão TCP falha na hora; a autorização é presumida e registrada no log ("autorizacao presumida para evitar deadlock"). O morto não disputa a SC, então a presunção é segura.
* **Par congelado (omissão)**: o pedido expira por timeout (RA_TIMEOUT) e o heartbeat o marca como fora; as próximas entradas na SC o pulam sem custo, até ele voltar a responder.
* **Autorização antiga**: a permissão de SC tem validade (lease de 10 s). Um Sync retomado após uma pausa longa (unpause/SIGCONT) detecta que sua autorização expirou, descarta-a e **reentra na SC** em vez de usar a permissão velha.

## Protocolo 2 (cópia primária móvel): fluxo completo de uma escrita

1. **W1**: cliente envia a escrita ao seu Sync.
2. **Entrada na SC**: o Sync obtém a exclusão mútua distribuída.
3. **Escolha do Store**: o Sync sorteia um elemento do Cluster Store (pode variar a cada entrada na SC, como o enunciado permite).
4. **W2**: se o Store escolhido não é o primário do item, ele pede a cópia primária ao dono atual, que a **entrega e autoriza** a migração (com REDIRECT se a primária já tiver migrado adiante). Um Store **não se autopromove por timeout**: se o dono está fora do ar, o novo primário adota a versão mais recente entre as réplicas vivas, e a segurança contra dois primários simultâneos vem da serialização imposta pela SC (nunca há duas migrações concorrentes do mesmo item).
5. **Escrita local**: o novo primário aplica a escrita (versão incrementada), com **idempotência por reqid**: uma requisição reenviada (mesmo reqid) é reconhecida sem ser aplicada de novo.
6. **W3**: o ACK volta ao Sync, que sai da SC e reconhece a escrita concluída ao cliente. O protocolo é não bloqueador: o cliente é liberado antes da propagação.
7. **W4**: o primário diz aos backups para atualizar (em paralelo, com o reqid junto).
8. **W5**: cada backup reconhece a atualização. Em seguida o primário **testa a consistência**: lê o item em todas as réplicas saudáveis e confirma valor e versão idênticos (`[CONSISTENCIA] OK` no log).

### Mensagens do protocolo

| Mensagem | Direção | Significado |
|---|---|---|
| `W1\|cliente\|item\|valor\|reqid` | Cliente -> Sync | Requisição de escrita |
| `RA_REQ\|ts\|id` / `RA_REP\|id` | Sync <-> Sync | Pedido e autorização de seção crítica |
| `WRITE\|item\|valor\|reqid\|sync` | Sync -> Store | Escrita no novo primário desejado |
| `TRANSFER\|item\|novo` / `ITEM\|...` / `REDIRECT\|dono` | Store <-> Store | W2: migração autorizada da primária |
| `ACK_WRITE\|item\|versao` | Store -> Sync | Escrita aplicada (gera o W3 ao cliente) |
| `UPDATE\|item\|valor\|versao\|primario\|reqid` | Store -> Store | W4: atualização dos backups |
| `ACK_UPDATE\|item\|versao` | Store -> Store | W5: reconhecimento do backup |
| `READ\|item` / `VALUE\|item\|valor\|versao\|primario\|reqid` | qualquer -> Store | Leitura (consistência, recuperação) |
| `PING` / `PONG` | qualquer | Detecção de falhas |

## Detecção e tolerância a falhas (queda e omissão)

* **Timeouts** em todas as comunicações (1,5 s padrão; valores maiores onde a operação legitimamente demora).
* **PING periódico** em três camadas: do cliente ao seu Sync antes de cada operação; **heartbeat contínuo entre os Syncs** (PING a cada 1 s; dois PINGs perdidos marcam o par como fora, um respondido o devolve), consultado pela entrada na SC para pular mortos sem pagar timeout; e verificador de saúde do monitor (PING a todos os nós a cada 2 s).
* **Resolução de nomes com cache e aquecimento**: `getaddrinfo` não tem timeout e o DNS do Docker pode travar com containers mortos; cada nó resolve todos os hosts na partida e reutiliza os endereços. O compose ainda fixa IPs estáticos por serviço, eliminando o problema na raiz.
* **Opção 1 do enunciado (falha de elemento do Cluster Sync), nos três estados**: o cliente não percebe a falha; ele detecta o silêncio (PING ou timeout do W1), redireciona-se sozinho a outro Sync e **reenvia o mesmo reqid**, que o Store deduplica.
  * 1.1 Sync ocioso: os demais presumem a autorização de SC do morto; nada trava.
  * 1.2 Sync com pedido do cliente: cliente mascara e reenvia.
  * 1.3 Sync dentro da SC: os demais recuperam a SC (queda: conexão cai na hora; omissão: timeout + lista de suspeitos) e o cliente mascara e reenvia. O lease impede que o Sync retomado use a autorização antiga.
* **Falha de Store**: detectada no encaminhamento, no W2 ou no W4; o sistema segue com as réplicas saudáveis, e um Store religado **recupera o estado junto ao cluster** (valor, versão, primário e reqid de cada item) antes de atender.

## Estrutura do projeto

```
├── Makefile
├── Dockerfile             (imagem dos nos C++)
├── docker-compose.yml     (1 monitor, 3 Stores, 5 Syncs, 5 Clientes)
├── include/common.hpp     (portas, constantes de SC, sockets com timeout)
├── src/
│   ├── client.cpp         (W1, PING, failover transparente, reenvio de reqid)
│   ├── sync_node.cpp      (Ricart-Agrawala, lease, W1 -> Store -> W3)
│   └── store_node.cpp     (W2 seguro, escrita idempotente, W4/W5, consistencia,
│                           recuperacao pos-queda)
├── monitor/               (painel web ao vivo + injecao de falhas reais)
├── scripts/               (run_all.sh, kill_node.sh, stop_all.sh)
└── tests/                 (suite automatizada; ver abaixo)
```

## Compilação e execução

### Modo local

```bash
make
./scripts/run_all.sh          # monitor + 3 Stores + 5 Syncs + 5 clientes continuos
# painel: http://localhost:8080
./scripts/kill_node.sh sync 0           # queda
./scripts/kill_node.sh store 1 omissao  # omissao (SIGSTOP)
./scripts/stop_all.sh
```

Falhas programadas para demonstrar a Opção 1 de forma determinística:

```bash
./bin/sync_node 0 falhar-apos-w1   # cai apos receber um W1 (caso 1.2)
./bin/sync_node 0 falhar-na-sc     # cai dentro da secao critica (caso 1.3)
./bin/sync_node 0 falhar-antes-w3  # cai antes do W3 (exercita a idempotencia)
```

### Modo Docker Compose

```bash
docker compose up --build -d
# painel: http://localhost:8080  (clique nos nos para injetar falhas reais)
docker compose kill sync0        # queda
docker compose pause store1      # omissao
docker compose unpause store1
docker compose start sync0
docker compose down
```

## Testes automatizados

```bash
./tests/run_tests.sh    # executa a suite completa e resume PASS/FAIL
```

| Teste | O que valida |
|---|---|
| test1_normal.sh | Execução normal: W1 a W5 nos logs, entrada/saída da SC, consistência interna e por leitura externa das réplicas |
| test2_falha_sync.sh | Opção 1 nos três estados (1.1 ocioso, 1.2 com pedido, 1.3 dentro da SC): falha mascarada, escritas concluídas, SC recuperada sem deadlock |
| test3_omissao.sh | Omissão (SIGSTOP) no Sync do cliente e num Store durante a replicação; retorno com SIGCONT |
| test4_concorrencia.sh | 5 clientes simultâneos: 20 escritas concluídas, exclusão mútua respeitada (linha do tempo global de ENTROU/SAIU sem sobreposição), réplicas idênticas |
| test5_idempotencia.sh | Sync cai após a escrita e antes do W3; o reenvio do mesmo reqid NÃO é reaplicado (versão final v1) |
| test6_queda_store.sh | Queda do Store primário: migração segura sem autopromoção por timeout, e recuperação de estado ao religar |

## Limitações conhecidas (honestas, para o relatório)

* O detector de falhas é baseado em timeout (modelo de queda e omissão, como o enunciado pede). Se um Sync saudável demorar mais que RA_TIMEOUT numa resposta adiada (por exemplo, com todos os Stores fora do ar), um par pode presumi-lo morto e a exclusão mútua fica probabilística nesse cenário degradado; os valores foram calibrados para que isso não ocorra em operação normal.
* O monitor é uma ferramenta de observação e demonstração; o sistema funciona integralmente sem ele.
