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
#include <stdexcept>

struct Item {
    std::string valor = "0";
    long versao = 0;
    std::string ultimo_reqid;   // ultima requisicao de escrita aplicada (dedup de retentativas)
};

static int MEU_ID = -1;
static std::string TAG;

static std::map<std::string, Item> g_dados;        // copia de todos os recursos
static std::map<std::string, int>  g_primario;     // item -> id do Store primario
static std::mutex g_item_mtx[NUM_ITENS];           // uma trava por item: serializa migracao de
                                                    // primaria e escrita para o mesmo recurso

static std::string item_nome(int i) { return "R" + std::to_string(i); }

// Todos os itens sao pre-cadastrados em main() antes de qualquer thread iniciar,
// entao g_dados/g_primario nunca sofrem insercao/remocao concorrente; .at() garante
// isso estaticamente (nunca insere) e valida o indice usado para travar por item.
static int item_idx(const std::string& item) {
    if (item.size() < 2 || item[0] != 'R')
        throw std::invalid_argument("item invalido: " + item);
    int idx = std::stoi(item.substr(1));
    if (idx < 0 || idx >= NUM_ITENS)
        throw std::out_of_range("item fora do intervalo: " + item);
    return idx;
}

// Propagacao W4/W5 (em paralelo, um Store por thread) + teste de consistencia.
// Executa apos responder ao Sync, caracterizando o protocolo nao bloqueador do
// Protocolo 2. Chamada com a trava do item ja adquirida pelo chamador.
static void propagar_e_verificar(const std::string& item, const std::string& valor,
                                 long versao, const std::string& reqid) {
    std::vector<int> saudaveis;
    std::mutex mtx_saudaveis;
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_STORE; i++) {
        if (i == MEU_ID) continue;
        threads.emplace_back([&, i]() {
            std::string req = "UPDATE|" + item + "|" + valor + "|" + std::to_string(versao)
                              + "|" + std::to_string(MEU_ID) + "|" + reqid;
            std::string resp;
            logmsg(TAG, "W4: dizendo ao backup Store " + std::to_string(i)
                        + " para atualizar " + item + " (v" + std::to_string(versao) + ")");
            bool ok = rpc(store_host(i), store_port(i), req, resp);
            auto p = ok ? split(resp) : std::vector<std::string>{};
            if (ok && !p.empty() && p[0] == "ACK_UPDATE") {
                logmsg(TAG, "W5: Store " + std::to_string(i)
                            + " reconheceu a atualizacao de " + item);
                std::lock_guard<std::mutex> lk(mtx_saudaveis);
                saudaveis.push_back(i);
            } else {
                logmsg(TAG, "[FALHA DETECTADA] Store " + std::to_string(i)
                            + " sem resposta ao W4 (queda ou omissao). Seguindo com as replicas saudaveis.");
            }
        });
    }
    for (auto& t : threads) t.join();

    // Teste de consistencia entre as replicas saudaveis
    bool ok = true;
    for (int i : saudaveis) {
        std::string resp;
        if (!rpc(store_host(i), store_port(i), "READ|" + item, resp)) { ok = false; continue; }
        auto p = split(resp); // VALUE|item|valor|versao|primario
        if (p.size() < 4 || p[2] != valor || std::stol(p[3]) != versao) ok = false;
    }
    if (ok)
        logmsg(TAG, "[CONSISTENCIA] OK: replicas saudaveis identicas para " + item
                    + " (valor=" + valor + ", v" + std::to_string(versao) + ")");
    else
        logmsg(TAG, "[CONSISTENCIA] DIVERGENCIA detectada em " + item + "!");
}

// Garante que este Store e o primario do item, executando o W2 se preciso: pede a
// copia primaria ao dono anterior conhecido localmente. Se esse dono ja tiver
// repassado a primaria para outro Store nesse meio-tempo (corrida entre duas
// migracoes concorrentes para o mesmo item), ele responde REDIRECT e este Store
// segue o ponteiro ate achar o dono atual ou esgotar as tentativas. Chamada com a
// trava do item ja adquirida pelo chamador (serializa migracoes concorrentes deste
// Store para o mesmo item).
static void assumir_primaria(const std::string& item) {
    int antigo = g_primario.at(item);
    if (antigo == MEU_ID) return;

    bool obteve = false;
    std::string valor_recebido;
    long versao_recebida = 0;
    std::string reqid_recebido;
    for (int hop = 0; hop < NUM_STORE && antigo != MEU_ID; hop++) {
        logmsg(TAG, "W2: pedindo ao Store " + std::to_string(antigo)
                    + " a copia primaria de " + item);
        std::string resp;
        if (!rpc(store_host(antigo), store_port(antigo),
                 "TRANSFER|" + item + "|" + std::to_string(MEU_ID), resp)) {
            logmsg(TAG, "[FALHA DETECTADA] Store " + std::to_string(antigo)
                        + " sem resposta ao W2 (queda ou omissao).");
            // Um Store NAO se autopromove apenas por timeout: consulta as demais
            // replicas vivas e adota a versao mais recente disponivel. A seguranca
            // contra dois primarios simultaneos vem da secao critica do Cluster
            // Sync (Ricart-Agrawala), que serializa as escritas: nunca ha duas
            // migracoes concorrentes para o mesmo item.
            long melhor_v = -1;
            std::string melhor_val, melhor_rq;
            for (int s = 0; s < NUM_STORE; s++) {
                if (s == MEU_ID || s == antigo) continue;
                std::string r2;
                if (!rpc(store_host(s), store_port(s), "READ|" + item, r2, 800)) continue;
                auto q = split(r2); // VALUE|item|valor|versao|primario|reqid
                if (q.size() >= 4 && std::stol(q[3]) > melhor_v) {
                    melhor_v   = std::stol(q[3]);
                    melhor_val = q[2];
                    melhor_rq  = (q.size() >= 6) ? q[5] : "";
                }
            }
            if (melhor_v >= 0) {
                valor_recebido = melhor_val;
                versao_recebida = melhor_v;
                reqid_recebido = melhor_rq;
                obteve = true;
                logmsg(TAG, "Assumindo a primaria de " + item + " de forma segura:"
                            " adotada a versao mais recente entre as replicas vivas (v"
                            + std::to_string(melhor_v) + "), sob a serializacao da SC do Sync");
            } else {
                logmsg(TAG, "Nenhuma outra replica respondeu; assumindo a primaria de "
                            + item + " com a copia local (unica replica viva), sob a SC do Sync");
            }
            break;
        }
        auto p = split(resp);
        if (!p.empty() && p[0] == "ITEM" && p.size() >= 5) {
            valor_recebido = p[2];
            versao_recebida = std::stol(p[3]);
            reqid_recebido = p[4];
            obteve = true;
            break;
        } else if (!p.empty() && p[0] == "REDIRECT" && p.size() >= 2) {
            int indicado = std::stoi(p[1]);
            logmsg(TAG, "Store " + std::to_string(antigo) + " ja repassou a primaria de "
                        + item + "; seguindo para o Store " + std::to_string(indicado));
            antigo = indicado;
        } else {
            break;
        }
    }
    if (obteve) {
        Item& it = g_dados.at(item);
        it.valor = valor_recebido;
        it.versao = versao_recebida;
        it.ultimo_reqid = reqid_recebido;
    }
    g_primario.at(item) = MEU_ID;
    // Avisa os demais Stores sobre o novo primario (best effort)
    for (int i = 0; i < NUM_STORE; i++) {
        if (i == MEU_ID) continue;
        std::string r;
        rpc(store_host(i), store_port(i), "NEWPRIM|" + item + "|" + std::to_string(MEU_ID), r, 500);
    }
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

        } else if (cmd == "WRITE" && p.size() >= 5) {
            // WRITE|item|valor|reqid|sync_id  (encaminhado pelo Cluster Sync)
            const std::string item = p[1], valor = p[2], reqid = p[3], sync = p[4];
            std::lock_guard<std::mutex> critica(g_item_mtx[item_idx(item)]);
            assumir_primaria(item);
            Item& it = g_dados.at(item);
            bool duplicada = !reqid.empty() && it.ultimo_reqid == reqid;
            if (!duplicada) {
                it.valor = valor;
                it.versao += 1;
                it.ultimo_reqid = reqid;
            }
            long versao = it.versao;
            if (duplicada)
                logmsg(TAG, "Requisicao " + reqid + " ja aplicada (retentativa apos falha do Sync"
                            " ou do cliente); reconhecendo " + item + " sem reescrever (v"
                            + std::to_string(versao) + ")");
            else
                logmsg(TAG, "Escrita aplicada localmente como primario: " + item + " = "
                            + valor + " (v" + std::to_string(versao) + ", req " + reqid
                            + ", via Sync " + sync + ")");
            // Protocolo nao bloqueador: reconhece antes de propagar aos backups
            send_line(conn, "ACK_WRITE|" + item + "|" + std::to_string(versao));
            if (!duplicada) propagar_e_verificar(item, valor, versao, reqid);

        } else if (cmd == "TRANSFER" && p.size() >= 3) {
            // TRANSFER|item|novo_primario
            const std::string item = p[1];
            int novo = std::stoi(p[2]);
            std::lock_guard<std::mutex> critica(g_item_mtx[item_idx(item)]);
            if (g_primario.at(item) != MEU_ID) {
                // Ja repassei a primaria deste item para outro Store (corrida com
                // outra migracao concorrente): informo quem e o dono atual para o
                // requisitante seguir o ponteiro em vez de receber uma copia velha.
                send_line(conn, "REDIRECT|" + std::to_string(g_primario.at(item)));
            } else {
                Item copia = g_dados.at(item);
                g_primario.at(item) = novo;
                logmsg(TAG, "W2: entregando a copia primaria de " + item + " ao Store "
                            + std::to_string(novo) + " (v" + std::to_string(copia.versao) + ")");
                send_line(conn, "ITEM|" + item + "|" + copia.valor + "|"
                                + std::to_string(copia.versao) + "|" + copia.ultimo_reqid);
            }

        } else if (cmd == "NEWPRIM" && p.size() >= 3) {
            const std::string item = p[1];
            std::lock_guard<std::mutex> critica(g_item_mtx[item_idx(item)]);
            g_primario.at(item) = std::stoi(p[2]);
            send_line(conn, "OK");

        } else if (cmd == "UPDATE" && p.size() >= 6) {
            // UPDATE|item|valor|versao|primario|reqid  (W4 vindo do primario)
            const std::string item = p[1], valor = p[2];
            long versao = std::stol(p[3]);
            int primario_id = std::stoi(p[4]);
            const std::string reqid = p[5];
            logmsg(TAG, "W4 recebido do primario Store " + p[4] + ": atualizando backup "
                        + item + " para v" + std::to_string(versao));
            {
                std::lock_guard<std::mutex> critica(g_item_mtx[item_idx(item)]);
                Item& it = g_dados.at(item);
                if (versao >= it.versao) {
                    it.valor = valor;
                    it.versao = versao;
                    it.ultimo_reqid = reqid;
                }
                g_primario.at(item) = primario_id;
            }
            send_line(conn, "ACK_UPDATE|" + item + "|" + std::to_string(versao));
            logmsg(TAG, "W5: reconhecimento de atualizacao enviado ao primario (item "
                        + item + ")");

        } else if (cmd == "READ" && p.size() >= 2) {
            const std::string item = p[1];
            Item copia;
            int primario;
            {
                std::lock_guard<std::mutex> critica(g_item_mtx[item_idx(item)]);
                copia = g_dados.at(item);
                primario = g_primario.at(item);
            }
            send_line(conn, "VALUE|" + item + "|" + copia.valor + "|"
                            + std::to_string(copia.versao) + "|" + std::to_string(primario)
                            + "|" + copia.ultimo_reqid);

        } else {
            send_line(conn, "ERRO|comando_invalido");
        }
    } catch (const std::exception& e) {
        logmsg(TAG, "[ERRO] mensagem malformada recebida, conexao encerrada (" + std::string(e.what()) + ")");
    }
    ::close(conn);
}

// Ao entrar (ou reentrar apos uma queda), busca nas outras replicas o estado mais
// recente de cada item em vez de assumir os valores padrao: evita que um Store
// reiniciado acredite ser o primario de um item cuja primaria ja migrou enquanto
// ele estava fora, o que faria com que ele aplicasse escritas sobre dados obsoletos.
static void sincronizar_com_cluster() {
    for (int i = 0; i < NUM_ITENS; i++) {
        std::string item = item_nome(i);
        bool achou = false;
        long melhor_versao = -1;
        std::string melhor_valor = "0";
        int melhor_primario = 0;
        std::string melhor_reqid;
        for (int s = 0; s < NUM_STORE; s++) {
            if (s == MEU_ID) continue;
            std::string resp;
            if (!rpc(store_host(s), store_port(s), "READ|" + item, resp, 800)) continue;
            auto p = split(resp); // VALUE|item|valor|versao|primario|reqid
            if (p.size() < 5) continue;
            long v = std::stol(p[3]);
            achou = true;
            if (v > melhor_versao) {
                melhor_versao = v;
                melhor_valor = p[2];
                melhor_primario = std::stoi(p[4]);
                melhor_reqid = (p.size() >= 6) ? p[5] : "";
            }
        }
        if (achou) {
            g_dados[item] = { melhor_valor, melhor_versao, melhor_reqid };
            g_primario[item] = melhor_primario;
            logmsg(TAG, "Estado recuperado de " + item + " junto ao cluster: valor="
                        + melhor_valor + ", v" + std::to_string(melhor_versao)
                        + ", primario atual = Store " + std::to_string(melhor_primario));
        }
    }
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "Uso: %s <id 0..2>\n", argv[0]); return 1; }
    MEU_ID = std::atoi(argv[1]);
    TAG = "Store " + std::to_string(MEU_ID);
    aquecer_dns();

    for (int i = 0; i < NUM_ITENS; i++) {
        g_dados[item_nome(i)] = Item{};
        g_primario[item_nome(i)] = 0; // primario inicial (primeira subida do cluster): Store 0
    }
    sincronizar_com_cluster();

    int srv = tcp_listen(store_port(MEU_ID));
    if (srv < 0) { logmsg(TAG, "Erro ao abrir porta"); return 1; }
    logmsg(TAG, "Ativo na porta " + std::to_string(store_port(MEU_ID))
                + " com copia de todos os recursos (replicacao " + std::to_string(NUM_STORE) + ")");

    while (true) {
        int conn = ::accept(srv, nullptr, nullptr);
        if (conn < 0) continue;
        set_conn_timeouts(conn);
        std::thread(atender, conn).detach();
    }
}
