// sync_node.cpp
// Elemento do Cluster Sync.
//
// Fluxo de uma escrita (Protocolo 2 + exclusao mutua do TP2):
//   W1 (cliente) -> entrar na secao critica distribuida (Ricart-Agrawala entre
//   os 5 Syncs) -> escolher um elemento do Cluster Store -> WRITE (dispara o W2
//   de migracao da primaria no Store) -> ACK -> sair da SC -> W3 ao cliente.
//
// Exclusao mutua (Ricart-Agrawala, TP2): para entrar na SC o Sync envia
// RA_REQ|ts|id a todos os pares e espera a autorizacao (RA_REP) de cada um.
// Um par que nao esta interessado (ou tem prioridade menor pela ordem total
// (ts, id)) responde na hora; um par na SC ou com prioridade adia a resposta,
// mantendo a conexao aberta, e so autoriza ao sair da SC.
//
// Tolerancia a falhas na SC:
// - Par morto (queda): a conexao falha e a autorizacao e presumida, evitando
//   deadlock (o par nao disputa a SC).
// - Par congelado (omissao): timeout de RA_TIMEOUT_MS; o par entra numa lista
//   de suspeitos e passa a ter espera reduzida ate voltar a responder.
// - Autorizacao antiga: a entrada na SC tem validade (LEASE_MS). Um Sync
//   retomado apos longa pausa (SIGCONT/unpause) percebe que sua autorizacao
//   expirou e reentra na SC em vez de usar a permissao velha.
//
// Falhas programadas (apenas para os testes automatizados da Opcao 1):
//   ./bin/sync_node <id> falhar-apos-w1  -> cai apos receber um W1 (caso 1.2)
//   ./bin/sync_node <id> falhar-na-sc    -> cai dentro da SC        (caso 1.3)
//   ./bin/sync_node <id> falhar-antes-w3 -> cai apos a escrita no Store e antes
//                                           do W3 (exercita a idempotencia)
// O caso 1.1 (Sync ocioso) dispensa flag: basta derrubar um Sync sem clientes.

#include "common.hpp"
#include <thread>
#include <random>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <vector>

static int MEU_ID = -1;
static std::string TAG;
static std::string FALHA_PROGRAMADA;          // "", falhar-apos-w1, falhar-na-sc, falhar-antes-w3
static std::atomic<bool> falha_armada{true};  // dispara uma unica vez

// ---------------- Estado do Ricart-Agrawala ----------------
static std::mutex ra_mtx;
static std::condition_variable ra_cv;
static long ra_clock = 0;          // relogio logico de Lamport
static bool ra_interessado = false;
static bool ra_na_sc = false;
static long ra_meu_ts = 0;
static std::chrono::steady_clock::time_point ra_concedida_em;

// ---- Heartbeat entre os Syncs ----
// Cada Sync monitora os pares continuamente (PING a cada 1 s). A entrada na SC
// consulta esse estado: um par ja sabidamente morto e pulado NA HORA (com a
// autorizacao presumida registrada), em vez de pagar o timeout a cada entrada.
// Dois PINGs perdidos seguidos marcam o par como fora; um PING respondido o
// devolve ao grupo. Todos comecam presumidos vivos (otimista), garantindo que
// nenhuma autorizacao e pulada indevidamente na partida.
static std::atomic<bool> par_vivo[NUM_SYNC];
static std::atomic<int>  par_falhas[NUM_SYNC];

static void registrar_falha_do_par(int j) {
    if (par_falhas[j].fetch_add(1) + 1 >= 2 && par_vivo[j].exchange(false))
        logmsg(TAG, "[FALHA DETECTADA] Sync " + std::to_string(j) +
                    " marcado como fora do ar pelo heartbeat (queda ou omissao)");
}
static void registrar_vida_do_par(int j) {
    par_falhas[j] = 0;
    if (!par_vivo[j].exchange(true))
        logmsg(TAG, "Sync " + std::to_string(j) + " voltou a responder ao heartbeat");
}

static void heartbeat_loop() {
    while (true) {
        for (int j = 0; j < NUM_SYNC; j++) {
            if (j == MEU_ID) continue;
            std::string resp;
            if (rpc(sync_host(j), sync_port(j), "PING", resp, 500) && resp == "PONG")
                registrar_vida_do_par(j);
            else
                registrar_falha_do_par(j);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

static bool falhar_agora(const char* qual) {
    return FALHA_PROGRAMADA == qual && falha_armada.exchange(false);
}

// Solicita a SC a todos os pares e bloqueia ate obter todas as autorizacoes
// (explicitas, ou presumidas quando o par esta fora do ar).
static void entrar_sc() {
    long ts;
    {
        std::lock_guard<std::mutex> lk(ra_mtx);
        ra_interessado = true;
        ra_meu_ts = ++ra_clock;
        ts = ra_meu_ts;
    }
    logmsg(TAG, "Solicitando entrada na secao critica (Ricart-Agrawala, ts=" +
                std::to_string(ts) + ")");
    std::vector<std::thread> pedidos;
    for (int j = 0; j < NUM_SYNC; j++) {
        if (j == MEU_ID) continue;
        if (!par_vivo[j]) {
            logmsg(TAG, "Sync " + std::to_string(j) + " fora do ar (heartbeat);"
                        " autorizacao presumida para evitar deadlock");
            continue;
        }
        pedidos.emplace_back([j, ts]() {
            std::string resp;
            bool ok = rpc(sync_host(j), sync_port(j),
                          "RA_REQ|" + std::to_string(ts) + "|" + std::to_string(MEU_ID),
                          resp, RA_TIMEOUT_MS);
            if (ok) {
                registrar_vida_do_par(j);
                logmsg(TAG, "Sync " + std::to_string(j) + " autorizou minha entrada na SC");
            } else {
                par_falhas[j] = 2; par_vivo[j] = false;
                logmsg(TAG, "[FALHA DETECTADA] Sync " + std::to_string(j) +
                            " sem resposta ao pedido de SC (queda ou omissao);"
                            " autorizacao presumida para evitar deadlock");
            }
        });
    }
    for (auto& t : pedidos) t.join();
    {
        std::lock_guard<std::mutex> lk(ra_mtx);
        ra_na_sc = true;
        ra_concedida_em = std::chrono::steady_clock::now();
    }
    logmsg(TAG, "ENTROU na secao critica");
}

static void sair_sc() {
    {
        std::lock_guard<std::mutex> lk(ra_mtx);
        ra_na_sc = false;
        ra_interessado = false;
    }
    ra_cv.notify_all();   // libera as autorizacoes adiadas
    logmsg(TAG, "SAIU da secao critica");
}

// A autorizacao continua valida? Um processo retomado apos pausa longa nao
// pode usar a permissao antiga: deve reentrar na SC.
static bool lease_valida() {
    using namespace std::chrono;
    std::lock_guard<std::mutex> lk(ra_mtx);
    return ra_na_sc &&
           duration_cast<milliseconds>(steady_clock::now() - ra_concedida_em).count() < LEASE_MS;
}

static int store_aleatorio() {
    static thread_local std::mt19937 rng(std::random_device{}() + MEU_ID);
    std::uniform_int_distribution<int> d(0, NUM_STORE - 1);
    return d(rng);
}

// Encaminha a escrita ao Cluster Store (com tolerancia a falha de Store) e
// devolve a versao aplicada, ou -1 se nenhum Store respondeu.
static long escrever_no_cluster_store(const std::string& item, const std::string& valor,
                                      const std::string& reqid) {
    int alvo = store_aleatorio();
    for (int tent = 0; tent < NUM_STORE; tent++, alvo = (alvo + 1) % NUM_STORE) {
        logmsg(TAG, "Encaminhando escrita de " + item + " ao Store "
                    + std::to_string(alvo) + " (novo primario desejado)");
        std::string req = "WRITE|" + item + "|" + valor + "|" + reqid + "|"
                          + std::to_string(MEU_ID);
        std::string resp;
        bool respondeu = rpc(store_host(alvo), store_port(alvo), req, resp, 4000);
        auto r = respondeu ? split(resp) : std::vector<std::string>{};
        if (respondeu && !r.empty() && r[0] == "ACK_WRITE" && r.size() >= 3)
            return std::stol(r[2]);
        logmsg(TAG, "[FALHA DETECTADA] Store " + std::to_string(alvo)
                    + " sem resposta (queda ou omissao). Tentando outro Store.");
    }
    return -1;
}

static void atender(int conn) {
    std::string linha;
    if (!recv_line(conn, linha)) { ::close(conn); return; }
    try {
        auto p = split(linha);
        if (p.empty()) { ::close(conn); return; }
        const std::string& cmd = p[0];

        if (cmd == "PING") {
            send_line(conn, "PONG");

        } else if (cmd == "RA_REQ" && p.size() >= 3) {
            // Pedido de SC de um par: RA_REQ|ts|id
            long ts = std::stol(p[1]);
            int outro = std::stoi(p[2]);
            std::unique_lock<std::mutex> lk(ra_mtx);
            ra_clock = std::max(ra_clock, ts) + 1;
            auto devo_adiar = [&]() {
                return ra_na_sc ||
                       (ra_interessado && (ra_meu_ts < ts ||
                                           (ra_meu_ts == ts && MEU_ID < outro)));
            };
            if (devo_adiar()) {
                logmsg(TAG, "SC em uso ou com prioridade minha: adiando autorizacao ao Sync "
                            + std::to_string(outro));
                ra_cv.wait(lk, [&]() { return !devo_adiar(); });
            }
            lk.unlock();
            send_line(conn, "RA_REP|" + std::to_string(MEU_ID));
            logmsg(TAG, "Autorizacao de SC concedida ao Sync " + std::to_string(outro));

        } else if (cmd == "W1" && p.size() >= 5) {
            // W1|cliente|item|valor|reqid
            const std::string cliente = p[1], item = p[2], valor = p[3], reqid = p[4];
            logmsg(TAG, "W1: requisicao de escrita recebida do Cliente " + cliente
                        + " (item=" + item + ", valor=" + valor + ", req " + reqid + ")");

            if (falhar_agora("falhar-apos-w1")) {
                logmsg(TAG, "[TESTE] Falha programada: caindo APOS receber o W1 (caso 1.2)");
                std::fflush(stdout); ::_exit(9);
            }

            // Exclusao mutua do TP2: so acessa o Cluster Store dentro da SC
            entrar_sc();
            if (falhar_agora("falhar-na-sc")) {
                logmsg(TAG, "[TESTE] Falha programada: caindo DENTRO da secao critica (caso 1.3)");
                std::fflush(stdout); ::_exit(9);
            }
            if (!lease_valida()) {
                logmsg(TAG, "Autorizacao de SC antiga ou expirada (processo possivelmente"
                            " retomado apos pausa); descartando-a e reentrando na SC");
                sair_sc();
                entrar_sc();
            }

            long versao = escrever_no_cluster_store(item, valor, reqid);

            if (falhar_agora("falhar-antes-w3")) {
                logmsg(TAG, "[TESTE] Falha programada: caindo APOS a escrita no Store e"
                            " ANTES do W3 (exercita idempotencia por reqid)");
                std::fflush(stdout); ::_exit(9);
            }

            sair_sc();

            if (versao >= 0) {
                logmsg(TAG, "W3: reconhecendo escrita concluida ao Cliente " + cliente
                            + " (item=" + item + ", v" + std::to_string(versao) + ")");
                send_line(conn, "W3|" + item + "|" + std::to_string(versao));
            } else {
                logmsg(TAG, "Nenhum Store disponivel para " + item);
                send_line(conn, "ERRO|store_indisponivel");
            }
        } else {
            send_line(conn, "ERRO|comando_invalido");
        }
    } catch (const std::exception& e) {
        logmsg(TAG, "[ERRO] mensagem malformada recebida, conexao encerrada ("
                    + std::string(e.what()) + ")");
    }
    ::close(conn);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr,
            "Uso: %s <id 0..4> [falhar-apos-w1|falhar-na-sc|falhar-antes-w3]\n", argv[0]);
        return 1;
    }
    MEU_ID = std::atoi(argv[1]);
    if (argc >= 3) FALHA_PROGRAMADA = argv[2];
    TAG = "Sync " + std::to_string(MEU_ID);
    for (int i = 0; i < NUM_SYNC; i++) { par_vivo[i] = true; par_falhas[i] = 0; }
    aquecer_dns();

    int srv = tcp_listen(sync_port(MEU_ID));
    if (srv < 0) { logmsg(TAG, "Erro ao abrir porta"); return 1; }
    logmsg(TAG, "Ativo na porta " + std::to_string(sync_port(MEU_ID))
                + (FALHA_PROGRAMADA.empty() ? "" : " [falha programada: " + FALHA_PROGRAMADA + "]"));
    std::thread(heartbeat_loop).detach();

    while (true) {
        int conn = ::accept(srv, nullptr, nullptr);
        if (conn < 0) continue;
        set_conn_timeouts(conn);
        std::thread(atender, conn).detach();
    }
}
