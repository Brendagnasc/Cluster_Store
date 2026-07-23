// sync_node.cpp
// Elemento do Cluster Sync. Recebe a requisicao de escrita do cliente (W1),
// escolhe um elemento do Cluster Store (pode variar a cada entrada na secao
// critica), encaminha a escrita e, ao receber o ACK do novo primario, envia o
// reconhecimento de escrita concluida (W3) ao cliente.
// Se o Store escolhido nao responder (queda/omissao), tenta outro Store.
//
// Uso: ./sync_node <id>   (id em 0..4)

#include "common.hpp"
#include <thread>
#include <random>

static int MEU_ID = -1;
static std::string TAG;

static int store_aleatorio() {
    static thread_local std::mt19937 rng(std::random_device{}() + MEU_ID);
    std::uniform_int_distribution<int> d(0, NUM_STORE - 1);
    return d(rng);
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

        } else if (cmd == "W1" && p.size() >= 5) {
            // W1|cliente|item|valor|reqid
            const std::string cliente = p[1], item = p[2], valor = p[3], reqid = p[4];
            logmsg(TAG, "W1: requisicao de escrita recebida do Cliente " + cliente
                        + " (item=" + item + ", valor=" + valor + ", req " + reqid + ")");

            // Escolhe um Store; em caso de falha, tenta os demais (tolerancia a falhas)
            int alvo = store_aleatorio();
            std::string resp;
            bool ok = false;
            for (int tent = 0; tent < NUM_STORE && !ok; tent++, alvo = (alvo + 1) % NUM_STORE) {
                logmsg(TAG, "Encaminhando escrita de " + item + " ao Store "
                            + std::to_string(alvo) + " (novo primario desejado)");
                std::string req = "WRITE|" + item + "|" + valor + "|" + reqid + "|"
                                  + std::to_string(MEU_ID);
                auto respondeu = rpc(store_host(alvo), store_port(alvo), req, resp, 4000);
                auto r = respondeu ? split(resp) : std::vector<std::string>{};
                if (respondeu && !r.empty() && r[0] == "ACK_WRITE" && r.size() >= 3) {
                    ok = true;
                } else {
                    logmsg(TAG, "[FALHA DETECTADA] Store " + std::to_string(alvo)
                                + " sem resposta (queda ou omissao). Tentando outro Store.");
                }
            }

            if (ok) {
                auto r = split(resp); // ACK_WRITE|item|versao
                logmsg(TAG, "W3: reconhecendo escrita concluida ao Cliente " + cliente
                            + " (item=" + item + ", v" + r[2] + ")");
                send_line(conn, "W3|" + item + "|" + r[2]);
            } else {
                logmsg(TAG, "Nenhum Store disponivel para " + item);
                send_line(conn, "ERRO|store_indisponivel");
            }
        } else {
            send_line(conn, "ERRO|comando_invalido");
        }
    } catch (const std::exception& e) {
        logmsg(TAG, "[ERRO] mensagem malformada recebida, conexao encerrada (" + std::string(e.what()) + ")");
    }
    ::close(conn);
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "Uso: %s <id 0..4>\n", argv[0]); return 1; }
    MEU_ID = std::atoi(argv[1]);
    TAG = "Sync " + std::to_string(MEU_ID);

    int srv = tcp_listen(sync_port(MEU_ID));
    if (srv < 0) { logmsg(TAG, "Erro ao abrir porta"); return 1; }
    logmsg(TAG, "Ativo na porta " + std::to_string(sync_port(MEU_ID)));

    while (true) {
        int conn = ::accept(srv, nullptr, nullptr);
        if (conn < 0) continue;
        std::thread(atender, conn).detach();
    }
}
