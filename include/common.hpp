#pragma once
// common.hpp
// Utilitarios compartilhados: configuracao de portas, logging com timestamp,
// split de mensagens e sockets TCP com timeout (deteccao de queda/omissao).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <string>
#include <vector>
#include <sstream>
#include <mutex>
#include <chrono>

// ---------------- Configuracao global ----------------
constexpr int NUM_SYNC   = 5;      // 5 elementos do Cluster Sync
constexpr int NUM_STORE  = 3;      // 3 elementos do Cluster Store (replicacao 3)
constexpr int NUM_ITENS  = 5;      // recursos R0..R4
constexpr int SYNC_BASE_PORT  = 8001;  // Sync i escuta em 8001+i
constexpr int STORE_BASE_PORT = 9001;  // Store i escuta em 9001+i
constexpr int TIMEOUT_MS = 1500;   // timeout padrao para detectar queda/omissao

inline int sync_port(int id)  { return SYNC_BASE_PORT + id; }
inline int store_port(int id) { return STORE_BASE_PORT + id; }

// ---------------- Logging ----------------
inline std::string timestamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms  = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    std::time_t t = system_clock::to_time_t(now);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%H:%M:%S", std::localtime(&t));
    char out[48];
    std::snprintf(out, sizeof(out), "%s.%03d", buf, (int)ms.count());
    return out;
}

inline void logmsg(const std::string& quem, const std::string& msg) {
    static std::mutex m;
    std::lock_guard<std::mutex> lk(m);
    std::printf("[%s][%s] %s\n", timestamp().c_str(), quem.c_str(), msg.c_str());
    std::fflush(stdout);
}

// ---------------- Mensagens ----------------
// Formato: CMD|arg1|arg2|... terminado em '\n'
inline std::vector<std::string> split(const std::string& s, char sep = '|') {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, sep)) out.push_back(tok);
    return out;
}

// ---------------- Sockets ----------------
// Conecta em 127.0.0.1:port com timeout (connect nao bloqueante + select).
inline int tcp_connect(int port, int timeout_ms = TIMEOUT_MS) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int r = ::connect(fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS) { ::close(fd); return -1; }
    if (r < 0) {
        fd_set wset; FD_ZERO(&wset); FD_SET(fd, &wset);
        timeval tv{ timeout_ms / 1000, (timeout_ms % 1000) * 1000 };
        if (select(fd + 1, nullptr, &wset, nullptr, &tv) <= 0) { ::close(fd); return -1; }
        int err = 0; socklen_t len = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err != 0) { ::close(fd); return -1; }
    }
    fcntl(fd, F_SETFL, flags); // volta a bloqueante
    timeval tv{ timeout_ms / 1000, (timeout_ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    return fd;
}

inline bool send_line(int fd, const std::string& msg) {
    std::string data = msg + "\n";
    size_t enviado = 0;
    while (enviado < data.size()) {
        ssize_t n = ::send(fd, data.data() + enviado, data.size() - enviado, MSG_NOSIGNAL);
        if (n <= 0) return false;
        enviado += (size_t)n;
    }
    return true;
}

// Le ate '\n'. Retorna false em timeout ou desconexao (omissao/queda).
inline bool recv_line(int fd, std::string& out) {
    out.clear();
    char c;
    while (true) {
        ssize_t n = ::recv(fd, &c, 1, 0);
        if (n <= 0) return false;          // timeout (SO_RCVTIMEO) ou conexao caiu
        if (c == '\n') return true;
        out.push_back(c);
    }
}

// Requisicao-resposta simples: conecta, envia, recebe uma linha, fecha.
inline bool rpc(int port, const std::string& req, std::string& resp,
                int timeout_ms = TIMEOUT_MS) {
    int fd = tcp_connect(port, timeout_ms);
    if (fd < 0) return false;
    bool ok = send_line(fd, req) && recv_line(fd, resp);
    ::close(fd);
    return ok;
}

// Cria socket de escuta em 0.0.0.0:port.
inline int tcp_listen(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);
    if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) return -1;
    if (::listen(fd, 64) < 0) return -1;
    return fd;
}
