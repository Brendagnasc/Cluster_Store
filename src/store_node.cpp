// store_node.cpp
// Elemento do Cluster Store (replica). Implementa o Protocolo 2 (primario dinamico):
// a copia primaria de um item migra para o Store que o Sync deseja atualizar (W2),
// a escrita e aplicada localmente e reconhecida (W3, via Sync), e as atualizacoes
// sao propagadas de forma nao bloqueadora aos backups (W4), que reconhecem (W5).
// Ao final, um teste de consistencia compara as replicas saudaveis.
//
// Uso: ./store_node <id>   (id em 0..2)

#include "common.hpp"
#include <map>
#include <thread>
#include <atomic>

struct Item {
    std::string valor = "0";
    long versao = 0;
};

static int MEU_ID = -1;
static std::string TAG;

static std::mutex g_mtx;
static std::map<std::string, Item> g_dados;        // copia de todos os recursos
static std::map<std::string, int>  g_primario;     // item -> id do Store primario

static std::string item_nome(int i) { return "R" + std::to_string(i); }

// Propagacao W4/W5 + teste de consistencia (executa apos responder ao Sync,
// caracterizando o protocolo nao bloqueador do Protocolo 2).
static void propagar_e_verificar(const std::string& item, const std::string& valor,
                                 long versao) {
    std::vector<int> saudaveis;
    for (int i = 0; i < NUM_STORE; i++) {
        if (i == MEU_ID) continue;
        std::string req = "UPDATE|" + item + "|" + valor + "|" + std::to_string(versao)
                          + "|" + std::to_string(MEU_ID);
        std::string resp;
        logmsg(TAG, "W4: dizendo ao backup Store " + std::to_string(i)
                    + " para atualizar " + item + " (v" + std::to_string(versao) + ")");
        if (rpc(store_host(i), store_port(i), req, resp) && split(resp)[0] == "ACK_UPDATE") {
            logmsg(TAG, "W5: Store " + std::to_string(i)
                        + " reconheceu a atualizacao de " + item);
            saudaveis.push_back(i);
        } else {
            logmsg(TAG, "[FALHA DETECTADA] Store " + std::to_string(i)
                        + " sem resposta ao W4 (queda ou omissao). Seguindo com as replicas saudaveis.");
        }
    }

    // Teste de consistencia entre as replicas saudaveis
    bool ok = true;
    for (int i : saudaveis) {
        std::string resp;
        if (!rpc(store_host(i), store_port(i), "READ|" + item, resp)) { ok = false; continue; }
        auto p = split(resp); // VALUE|item|valor|versao
        if (p.size() < 4 || p[2] != valor || std::stol(p[3]) != versao) ok = false;
    }
    if (ok)
        logmsg(TAG, "[CONSISTENCIA] OK: replicas saudaveis identicas para " + item
                    + " (valor=" + valor + ", v" + std::to_string(versao) + ")");
    else
        logmsg(TAG, "[CONSISTENCIA] DIVERGENCIA detectada em " + item + "!");
}

// Garante que este Store e o primario do item. Se nao for, executa o W2:
// move a copia primaria do antigo primario para ca e avisa os demais.
static void assumir_primaria(const std::string& item) {
    int antigo;
    {
        std::lock_guard<std::mutex> lk(g_mtx);
        antigo = g_primario[item];
        if (antigo == MEU_ID) return;
    }
    logmsg(TAG, "W2: movendo item " + item + " do primario antigo (Store "
                + std::to_string(antigo) + ") para o novo primario (Store "
                + std::to_string(MEU_ID) + ")");
    std::string resp;
    if (rpc(store_host(antigo), store_port(antigo),
            "TRANSFER|" + item + "|" + std::to_string(MEU_ID), resp)) {
        auto p = split(resp); // ITEM|item|valor|versao
        if (p.size() >= 4) {
            std::lock_guard<std::mutex> lk(g_mtx);
            g_dados[item] = { p[2], std::stol(p[3]) };
        }
    } else {
        logmsg(TAG, "[FALHA DETECTADA] Primario antigo Store " + std::to_string(antigo)
                    + " sem resposta. Assumindo a primaria com a copia local.");
    }
    {
        std::lock_guard<std::mutex> lk(g_mtx);
        g_primario[item] = MEU_ID;
    }
    // Avisa os demais Stores sobre o novo primario (best effort)
    for (int i = 0; i < NUM_STORE; i++) {
        if (i == MEU_ID || i == antigo) continue;
        std::string r;
        rpc(store_host(i), store_port(i), "NEWPRIM|" + item + "|" + std::to_string(MEU_ID), r, 500);
    }
}

static void atender(int conn) {
    std::string linha;
    if (!recv_line(conn, linha)) { ::close(conn); return; }
    auto p = split(linha);
    const std::string& cmd = p[0];

    if (cmd == "PING") {
        send_line(conn, "PONG");

    } else if (cmd == "WRITE" && p.size() >= 5) {
        // WRITE|item|valor|reqid|sync_id  (encaminhado pelo Cluster Sync)
        const std::string item = p[1], valor = p[2], reqid = p[3], sync = p[4];
        assumir_primaria(item);
        long versao;
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            Item& it = g_dados[item];
            it.valor  = valor;
            it.versao += 1;
            versao = it.versao;
        }
        logmsg(TAG, "Escrita aplicada localmente como primario: " + item + " = "
                    + valor + " (v" + std::to_string(versao) + ", req " + reqid
                    + ", via Sync " + sync + ")");
        // Protocolo nao bloqueador: reconhece antes de propagar aos backups
        send_line(conn, "ACK_WRITE|" + item + "|" + std::to_string(versao));
        propagar_e_verificar(item, valor, versao);

    } else if (cmd == "TRANSFER" && p.size() >= 3) {
        // TRANSFER|item|novo_primario
        const std::string item = p[1];
        int novo = std::stoi(p[2]);
        Item copia;
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            copia = g_dados[item];
            g_primario[item] = novo;
        }
        logmsg(TAG, "W2: entregando a copia primaria de " + item + " ao Store "
                    + std::to_string(novo) + " (v" + std::to_string(copia.versao) + ")");
        send_line(conn, "ITEM|" + item + "|" + copia.valor + "|"
                        + std::to_string(copia.versao));

    } else if (cmd == "NEWPRIM" && p.size() >= 3) {
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            g_primario[p[1]] = std::stoi(p[2]);
        }
        send_line(conn, "OK");

    } else if (cmd == "UPDATE" && p.size() >= 5) {
        // UPDATE|item|valor|versao|primario  (W4 vindo do primario)
        const std::string item = p[1], valor = p[2];
        long versao = std::stol(p[3]);
        logmsg(TAG, "W4 recebido do primario Store " + p[4] + ": atualizando backup "
                    + item + " para v" + std::to_string(versao));
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            Item& it = g_dados[item];
            if (versao >= it.versao) { it.valor = valor; it.versao = versao; }
            g_primario[item] = std::stoi(p[4]);
        }
        send_line(conn, "ACK_UPDATE|" + item + "|" + std::to_string(versao));
        logmsg(TAG, "W5: reconhecimento de atualizacao enviado ao primario (item "
                    + item + ")");

    } else if (cmd == "READ" && p.size() >= 2) {
        Item copia;
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            copia = g_dados[p[1]];
        }
        send_line(conn, "VALUE|" + p[1] + "|" + copia.valor + "|"
                        + std::to_string(copia.versao));
    }
    ::close(conn);
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "Uso: %s <id 0..2>\n", argv[0]); return 1; }
    MEU_ID = std::atoi(argv[1]);
    TAG = "Store " + std::to_string(MEU_ID);

    for (int i = 0; i < NUM_ITENS; i++) {
        g_dados[item_nome(i)] = Item{};
        g_primario[item_nome(i)] = 0; // primario inicial de todos os itens: Store 0
    }

    int srv = tcp_listen(store_port(MEU_ID));
    if (srv < 0) { logmsg(TAG, "Erro ao abrir porta"); return 1; }
    logmsg(TAG, "Ativo na porta " + std::to_string(store_port(MEU_ID))
                + " com copia de todos os recursos (replicacao " + std::to_string(NUM_STORE) + ")");

    while (true) {
        int conn = ::accept(srv, nullptr, nullptr);
        if (conn < 0) continue;
        std::thread(atender, conn).detach();
    }
}
