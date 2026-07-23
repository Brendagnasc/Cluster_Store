#include "common.hpp"
#include <thread>
#include <random>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <set>
#include <vector>

static int MEU_ID = -1;
static std::string TAG;
static std::string FALHA_PROGRAMADA;
static std::atomic<bool> falha_armada{true};

static std::mutex ra_mtx;
static std::condition_variable ra_cv;
static long ra_clock = 0;
static bool ra_interessado = false;
static bool ra_na_sc = false;
static long ra_meu_ts = 0;
static long ra_fence = 0;
static std::set<int> ra_pendentes;
static std::set<int> ra_adiados;
static std::chrono::steady_clock::time_point ra_concedida_em;

static std::atomic<bool> par_vivo[NUM_SYNC];
static std::atomic<int> par_falhas[NUM_SYNC];

static void remover_pendente(int j) {
    std::lock_guard<std::mutex> lk(ra_mtx);
    if (ra_pendentes.erase(j) > 0) ra_cv.notify_all();
}

static void registrar_falha_do_par(int j) {
    if (par_falhas[j].fetch_add(1) + 1 >= 2 && par_vivo[j].exchange(false)) {
        logmsg(TAG, "[FALHA DETECTADA] Sync " + std::to_string(j) +
                    " marcado como fora do ar pelo heartbeat");
        remover_pendente(j);
    }
}

static void registrar_vida_do_par(int j) {
    par_falhas[j] = 0;
    if (!par_vivo[j].exchange(true))
        logmsg(TAG, "Sync " + std::to_string(j) + " voltou a responder ao heartbeat");
}

static void heartbeat_loop() {
    while (true) {
        for (int j = 0; j < NUM_SYNC; ++j) {
            if (j == MEU_ID) continue;
            std::string resp;
            if (rpc(sync_host(j), sync_port(j), "PING", resp, 400) && resp == "PONG")
                registrar_vida_do_par(j);
            else
                registrar_falha_do_par(j);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(700));
    }
}

static bool falhar_agora(const char* qual) {
    return FALHA_PROGRAMADA == qual && falha_armada.exchange(false);
}

static void enviar_grant(int destino) {
    std::string resp;
    bool ok = rpc(sync_host(destino), sync_port(destino),
                  "RA_GRANT|" + std::to_string(MEU_ID), resp, 1000);
    if (ok) logmsg(TAG, "Autorizacao adiada enviada ao Sync " + std::to_string(destino));
    else registrar_falha_do_par(destino);
}

static void entrar_sc() {
    long ts;
    {
        std::lock_guard<std::mutex> lk(ra_mtx);
        ra_interessado = true;
        ra_meu_ts = ++ra_clock;
        ts = ra_meu_ts;
        ra_pendentes.clear();
        for (int j = 0; j < NUM_SYNC; ++j)
            if (j != MEU_ID && par_vivo[j]) ra_pendentes.insert(j);
    }

    logmsg(TAG, "Solicitando entrada na secao critica (Ricart-Agrawala, ts=" +
                std::to_string(ts) + ")");

    std::vector<std::thread> pedidos;
    for (int j = 0; j < NUM_SYNC; ++j) {
        if (j == MEU_ID || !par_vivo[j]) continue;
        pedidos.emplace_back([j, ts]() {
            std::string resp;
            bool ok = rpc(sync_host(j), sync_port(j),
                          "RA_REQ|" + std::to_string(ts) + "|" + std::to_string(MEU_ID),
                          resp, 1500);
            if (!ok) {
                registrar_falha_do_par(j);
                registrar_falha_do_par(j);
                return;
            }
            registrar_vida_do_par(j);
            auto p = split(resp);
            if (!p.empty() && p[0] == "RA_REP") {
                remover_pendente(j);
                logmsg(TAG, "Sync " + std::to_string(j) + " autorizou minha entrada na SC");
            } else if (!p.empty() && p[0] == "RA_DEFER") {
                logmsg(TAG, "Sync " + std::to_string(j) + " adiou a autorizacao");
            }
        });
    }
    for (auto& t : pedidos) t.join();

    {
        std::unique_lock<std::mutex> lk(ra_mtx);
        ra_cv.wait(lk, [] { return ra_pendentes.empty(); });
        ra_na_sc = true;
        ra_concedida_em = std::chrono::steady_clock::now();
        ra_fence = ra_meu_ts * NUM_SYNC + MEU_ID + 1;
    }
    logmsg(TAG, "ENTROU na secao critica (fencing token=" + std::to_string(ra_fence) + ")");
}

static void sair_sc() {
    std::set<int> liberar;
    {
        std::lock_guard<std::mutex> lk(ra_mtx);
        ra_na_sc = false;
        ra_interessado = false;
        liberar.swap(ra_adiados);
    }
    for (int j : liberar) std::thread(enviar_grant, j).detach();
    logmsg(TAG, "SAIU da secao critica");
}

static bool lease_valida() {
    using namespace std::chrono;
    std::lock_guard<std::mutex> lk(ra_mtx);
    return ra_na_sc && duration_cast<milliseconds>(steady_clock::now() - ra_concedida_em).count() < LEASE_MS;
}

static long fence_atual() {
    std::lock_guard<std::mutex> lk(ra_mtx);
    return ra_fence;
}

static int store_aleatorio() {
    static thread_local std::mt19937 rng(std::random_device{}() + MEU_ID);
    std::uniform_int_distribution<int> d(0, NUM_STORE - 1);
    return d(rng);
}

static long escrever_no_cluster_store(const std::string& item, const std::string& valor,
                                      const std::string& reqid, long fence) {
    int alvo = store_aleatorio();
    for (int tent = 0; tent < NUM_STORE; ++tent, alvo = (alvo + 1) % NUM_STORE) {
        if (!lease_valida()) return -2;
        logmsg(TAG, "Encaminhando escrita de " + item + " ao Store " + std::to_string(alvo));
        std::string req = "WRITE|" + item + "|" + valor + "|" + reqid + "|" +
                          std::to_string(MEU_ID) + "|" + std::to_string(fence);
        std::string resp;
        bool respondeu = rpc(store_host(alvo), store_port(alvo), req, resp, 3500);
        auto r = respondeu ? split(resp) : std::vector<std::string>{};
        if (respondeu && r.size() >= 3 && r[0] == "ACK_WRITE") return std::stol(r[2]);
        if (respondeu && !r.empty() && r[0] == "ERRO_FENCE") return -2;
        logmsg(TAG, "[FALHA DETECTADA] Store " + std::to_string(alvo) + " sem resposta");
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
            long ts = std::stol(p[1]);
            int outro = std::stoi(p[2]);
            bool adiar;
            {
                std::lock_guard<std::mutex> lk(ra_mtx);
                ra_clock = std::max(ra_clock, ts) + 1;
                adiar = ra_na_sc || (ra_interessado &&
                        (ra_meu_ts < ts || (ra_meu_ts == ts && MEU_ID < outro)));
                if (adiar) ra_adiados.insert(outro);
            }
            if (adiar) {
                send_line(conn, "RA_DEFER|" + std::to_string(MEU_ID));
                logmsg(TAG, "Autorizacao adiada ao Sync " + std::to_string(outro));
            } else {
                send_line(conn, "RA_REP|" + std::to_string(MEU_ID));
                logmsg(TAG, "Autorizacao concedida ao Sync " + std::to_string(outro));
            }
        } else if (cmd == "RA_GRANT" && p.size() >= 2) {
            int outro = std::stoi(p[1]);
            remover_pendente(outro);
            send_line(conn, "OK");
        } else if (cmd == "W1" && p.size() >= 5) {
            const std::string cliente = p[1], item = p[2], valor = p[3], reqid = p[4];
            logmsg(TAG, "W1: requisicao de escrita recebida do Cliente " + cliente + " (" + reqid + ")");
            if (falhar_agora("falhar-apos-w1")) { logmsg(TAG, "[TESTE] caindo APOS receber o W1"); ::_exit(9); }

            entrar_sc();
            if (falhar_agora("falhar-na-sc")) { logmsg(TAG, "[TESTE] caindo DENTRO da secao critica"); ::_exit(9); }

            if (!lease_valida()) { sair_sc(); entrar_sc(); }
            long fence = fence_atual();
            long versao = escrever_no_cluster_store(item, valor, reqid, fence);

            if (falhar_agora("falhar-antes-w3")) { logmsg(TAG, "[TESTE] caindo ANTES do W3"); ::_exit(9); }
            sair_sc();

            if (versao >= 0) send_line(conn, "W3|" + item + "|" + std::to_string(versao));
            else if (versao == -2) send_line(conn, "ERRO|autorizacao_expirada");
            else send_line(conn, "ERRO|store_indisponivel");
        } else {
            send_line(conn, "ERRO|comando_invalido");
        }
    } catch (const std::exception& e) {
        logmsg(TAG, "[ERRO] " + std::string(e.what()));
    }
    ::close(conn);
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "Uso: %s <id 0..4> [falha]\n", argv[0]); return 1; }
    MEU_ID = std::atoi(argv[1]);
    if (argc >= 3) FALHA_PROGRAMADA = argv[2];
    TAG = "Sync " + std::to_string(MEU_ID);
    for (int i = 0; i < NUM_SYNC; ++i) { par_vivo[i] = true; par_falhas[i] = 0; }
    aquecer_dns();
    int srv = tcp_listen(sync_port(MEU_ID));
    if (srv < 0) return 1;
    logmsg(TAG, "Ativo na porta " + std::to_string(sync_port(MEU_ID)));
    std::thread(heartbeat_loop).detach();
    while (true) {
        int conn = ::accept(srv, nullptr, nullptr);
        if (conn < 0) continue;
        set_conn_timeouts(conn, 5000);
        std::thread(atender, conn).detach();
    }
}
