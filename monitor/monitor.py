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
import socket
import threading

UDP_PORT = 7000
HTTP_PORT = 8080

_lock = threading.Lock()
_assinantes = []     # filas dos navegadores conectados
_historico = []      # ultimas linhas, para quem conectar depois


def udp_loop():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", UDP_PORT))
    while True:
        data, _ = s.recvfrom(65535)
        linha = data.decode("utf-8", "replace")
        with _lock:
            _historico.append(linha)
            if len(_historico) > 400:
                _historico.pop(0)
            for q in _assinantes:
                q.put(linha)


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

    def _envia(self, linha):
        self.wfile.write(b"data: " + json.dumps(linha).encode() + b"\n\n")
        self.wfile.flush()


if __name__ == "__main__":
    threading.Thread(target=udp_loop, daemon=True).start()
    print(f"Monitor no ar: http://localhost:{HTTP_PORT}  (recebendo eventos em UDP {UDP_PORT})")
    http.server.ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler).serve_forever()
