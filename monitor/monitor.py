#!/usr/bin/env python3
# monitor.py
# Monitor web do TP3. Recebe copias dos logs dos nos C++ via UDP (porta 7000)
# e as transmite ao navegador em tempo real via Server-Sent Events.
# Nao ha dependencias externas: apenas a biblioteca padrao do Python 3.
#
# Uso:  python3 monitor/monitor.py
# Painel: http://localhost:8080

import http.server
import json
import pathlib
import queue
import signal
import socket
import subprocess
import threading
import time

UDP_PORT = 7000
HTTP_PORT = 8080
RAIZ = pathlib.Path(__file__).resolve().parent.parent   # raiz do projeto

_lock = threading.Lock()
_assinantes = []     # filas dos navegadores conectados
_historico = []      # ultimas linhas, para quem conectar depois


def _broadcast(linha):
    with _lock:
        _historico.append(linha)
        if len(_historico) > 400:
            _historico.pop(0)
        for q in _assinantes:
            q.put(linha)


def _ts():
    return time.strftime("%H:%M:%S") + f".{int(time.time() * 1000) % 1000:03d}"


def _pid_de(tipo, nid):
    r = subprocess.run(["pgrep", "-f", f"bin/{tipo}_node {nid}$"],
                       capture_output=True, text=True)
    pids = [int(x) for x in r.stdout.split()]
    return pids[0] if pids else None


def _injetar_falha(tipo, nid, modo):
    """Aplica a falha real no processo: queda (kill -9), omissao (SIGSTOP)
    ou reativar (SIGCONT se congelado; relanca o binario se estava morto)."""
    nome = f"{'Sync' if tipo == 'sync' else 'Store'} {nid}"
    pid = _pid_de(tipo, nid)
    if modo == "queda":
        if pid is None:
            return f"{nome} ja esta fora do ar."
        subprocess.run(["kill", "-9", str(pid)])
        _broadcast(f"[{_ts()}][painel] Falha por QUEDA injetada em {nome} (kill -9, PID {pid}).")
    elif modo == "omissao":
        if pid is None:
            return f"{nome} nao esta rodando."
        subprocess.run(["kill", "-STOP", str(pid)])
        _broadcast(f"[{_ts()}][painel] Falha por OMISSAO injetada em {nome} (SIGSTOP, PID {pid}: vivo, porem mudo).")
    elif modo == "reativar":
        if pid is not None:
            subprocess.run(["kill", "-CONT", str(pid)])
            _broadcast(f"[{_ts()}][painel] {nome} reativado (SIGCONT: saiu da omissao).")
        else:
            log_path = RAIZ / "logs" / f"{tipo}{nid}.log"
            log_path.parent.mkdir(exist_ok=True)
            with open(log_path, "ab") as saida:
                subprocess.Popen([str(RAIZ / "bin" / f"{tipo}_node"), str(nid)],
                                 cwd=RAIZ, stdout=saida, stderr=subprocess.STDOUT)
            _broadcast(f"[{_ts()}][painel] {nome} relancado apos a queda.")
    return None


def udp_loop():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", UDP_PORT))
    while True:
        data, _ = s.recvfrom(65535)
        _broadcast(data.decode("utf-8", "replace"))


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):  # silencia o log de acesso do http.server
        pass

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            corpo = (pathlib.Path(__file__).parent / "dashboard.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(corpo)))
            self.end_headers()
            self.wfile.write(corpo)
        elif self.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            q = queue.Queue()
            with _lock:
                antigos = list(_historico)
                _assinantes.append(q)
            try:
                for linha in antigos:
                    self._envia(linha)
                while True:
                    try:
                        self._envia(q.get(timeout=15))
                    except queue.Empty:
                        self.wfile.write(b": ping\n\n")   # mantem a conexao viva
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with _lock:
                    _assinantes.remove(q)
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path != "/fail":
            self.send_error(404)
            return
        tamanho = int(self.headers.get("Content-Length", 0))
        try:
            corpo = json.loads(self.rfile.read(tamanho))
            tipo, nid, modo = corpo["tipo"], int(corpo["id"]), corpo["modo"]
            assert tipo in ("sync", "store") and modo in ("queda", "omissao", "reativar")
        except Exception:
            self.send_error(400, "requisicao invalida")
            return
        erro = _injetar_falha(tipo, nid, modo)
        resposta = json.dumps({"ok": erro is None, "erro": erro}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resposta)))
        self.end_headers()
        self.wfile.write(resposta)

    def _envia(self, linha):
        self.wfile.write(b"data: " + json.dumps(linha).encode() + b"\n\n")
        self.wfile.flush()


if __name__ == "__main__":
    threading.Thread(target=udp_loop, daemon=True).start()
    print(f"Monitor no ar: http://localhost:{HTTP_PORT}  (recebendo eventos em UDP {UDP_PORT})")
    http.server.ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler).serve_forever()
