// client.cpp
// Cliente. Conhece apenas um elemento do Cluster Sync por vez, escolhe
// randomicamente um recurso para escrever e envia W1. Antes de cada operacao
// envia um PING (mensagem periodica de controle) ao seu Sync; se o Sync nao
// responder (queda ou omissao), a falha e mascarada: o cliente e redirecionado
// de forma transparente para outro Sync e a operacao segue normalmente.
//
// Uso: ./client <id> [num_operacoes]   (id em 0..4)

#include "common.hpp"
#include <thread>
#include <random>

static int MEU_ID = -1;
static std::string TAG;
static int g_sync_atual = -1;   // unico Sync conhecido no momento

static std::mt19937 rng(std::random_device{}());

static std::string item_aleatorio() {
    std::uniform_int_distribution<int> d(0, NUM_ITENS - 1);
    return "R" + std::to_string(d(rng));
}

static int intervalo_ms() {
    std::uniform_int_distribution<int> d(500, 2000);
    return d(rng);
}

// Verifica o Sync atual com PING; em caso de silencio, redireciona (failover).
static void garantir_sync_vivo() {
    for (int tent = 0; tent < NUM_SYNC; tent++) {
        std::string resp;
        if (rpc(sync_host(g_sync_atual), sync_port(g_sync_atual), "PING", resp, 800) && resp == "PONG")
            return;
        int novo = (g_sync_atual + 1) % NUM_SYNC;
        logmsg(TAG, "[FALHA MASCARADA] Sync " + std::to_string(g_sync_atual)
                    + " sem resposta ao PING (queda ou omissao). Redirecionando de forma"
                      " transparente para o Sync " + std::to_string(novo) + ".");
        g_sync_atual = novo;
    }
}

// Envia W1 e aguarda W3. Se o Sync cair no meio da operacao, refaz a mesma
// requisicao (mesmo reqid) em outro Sync, sem o usuario perceber.
static bool escrever(const std::string& item, const std::string& valor,
                     const std::string& reqid) {
    for (int tent = 0; tent < NUM_SYNC; tent++) {
        garantir_sync_vivo();
        logmsg(TAG, "W1: enviando requisicao de escrita ao Sync "
                    + std::to_string(g_sync_atual) + " (item=" + item
                    + ", valor=" + valor + ", req " + reqid + ")");
        std::string resp;
        std::string req = "W1|" + std::to_string(MEU_ID) + "|" + item + "|" + valor
                          + "|" + reqid;
        if (rpc(sync_host(g_sync_atual), sync_port(g_sync_atual), req, resp, 8000)) {
            auto p = split(resp);
            if (p[0] == "W3" && p.size() >= 3) {
                logmsg(TAG, "W3 recebido: escrita de " + item + " concluida (v" + p[2]
                            + ") via Sync " + std::to_string(g_sync_atual));
                return true;
            }
        }
        int novo = (g_sync_atual + 1) % NUM_SYNC;
        logmsg(TAG, "[FALHA MASCARADA] Sync " + std::to_string(g_sync_atual)
                    + " caiu durante a operacao " + reqid + ". Reenviando a mesma"
                      " requisicao ao Sync " + std::to_string(novo)
                    + " de forma transparente.");
        g_sync_atual = novo;
    }
    logmsg(TAG, "Operacao " + reqid + " falhou em todos os Syncs.");
    return false;
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "Uso: %s <id 0..4> [ops, 0 = infinito]\n", argv[0]); return 1; }
    MEU_ID = std::atoi(argv[1]);
    int total_ops = (argc >= 3) ? std::atoi(argv[2]) : 0;   // padrao: infinito
    bool infinito = (total_ops <= 0);
    TAG = "Cliente " + std::to_string(MEU_ID);
    g_sync_atual = MEU_ID % NUM_SYNC;   // cada cliente conhece um Sync inicial

    logmsg(TAG, "Iniciado. Sync conhecido: Sync " + std::to_string(g_sync_atual)
                + (infinito ? " (operando continuamente)" : ""));

    for (int k = 1; infinito || k <= total_ops; k++) {
        std::string item  = item_aleatorio();
        std::string valor = "c" + std::to_string(MEU_ID) + "_op" + std::to_string(k);
        std::string reqid = "c" + std::to_string(MEU_ID) + "-" + std::to_string(k);
        escrever(item, valor, reqid);
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalo_ms()));
    }
    logmsg(TAG, "Finalizado (" + std::to_string(total_ops) + " operacoes).");
    return 0;
}
