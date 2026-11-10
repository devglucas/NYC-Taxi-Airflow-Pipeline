"""Servidor local que imita a fonte da TLC com defeito, para exercitar retries e alertas.

O primeiro trecho do caminho escolhe o defeito; o arquivo vem da cópia em `dados/raw`.
- /instavel: os dois primeiros GET respondem 503, depois entrega o arquivo.
- /truncado: promete o arquivo inteiro e corta na metade.
- /sem_coluna: arquivo legível, mas sem total_amount.
"""
from __future__ import annotations

import json
import tempfile
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import duckdb

RAW = Path("/opt/airflow/dados/raw")
CONTROLE = Path("/opt/airflow/dados/controle")
TEMP = Path(tempfile.mkdtemp())
tentativas: Counter[str] = Counter()


def sem_coluna(original: Path) -> Path:
    destino = TEMP / f"sem_coluna_{original.name}"
    if not destino.exists():
        with duckdb.connect() as con:
            con.execute(
                f"copy (select * exclude (total_amount) from read_parquet('{original}') limit 1000) "
                f"to '{destino}' (format parquet)"
            )
    return destino


class Manipulador(BaseHTTPRequestHandler):
    def _resolver(self) -> tuple[str, Path] | None:
        partes = self.path.strip("/").split("/")
        original = RAW / partes[-1]
        if len(partes) != 2 or not original.exists():
            self.send_error(404)
            return None
        return partes[0], original

    def _cabecalhos(self, status: int, tamanho: int, cenario: str, arquivo: Path) -> None:
        mes = arquivo.stem.rsplit("_", 1)[-1]
        estado = json.loads((CONTROLE / f"arquivo_{mes}.json").read_text()) if (CONTROLE / f"arquivo_{mes}.json").exists() else {}
        self.send_response(status)
        self.send_header("ETag", f'"{estado.get("etag") if cenario == "instavel" else cenario}"')
        self.send_header("Content-Length", str(tamanho))
        self.end_headers()

    def do_HEAD(self) -> None:
        resolvido = self._resolver()
        if resolvido:
            self._cabecalhos(200, resolvido[1].stat().st_size, resolvido[0], resolvido[1])

    def do_GET(self) -> None:
        resolvido = self._resolver()
        if not resolvido:
            return
        cenario, original = resolvido
        tentativas[cenario] += 1
        if cenario == "instavel" and tentativas[cenario] <= 2:
            self._cabecalhos(503, 0, cenario, original)
            return
        arquivo = sem_coluna(original) if cenario == "sem_coluna" else original
        corpo = arquivo.read_bytes()
        self._cabecalhos(200, len(corpo), cenario, original)
        self.wfile.write(corpo[: len(corpo) // 2] if cenario == "truncado" else corpo)

    def log_message(self, formato: str, *args) -> None:
        print(formato % args, flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Manipulador).serve_forever()
